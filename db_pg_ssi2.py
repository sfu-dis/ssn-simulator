import collections, random, sys, traceback
from dbsim import *

class EagerDoomedT1(AbortTransaction): pass
class EagerDoomedT2(AbortTransaction): pass
class PrecommitT2(AbortTransaction): pass

class ReadSkew(AbortTransaction):
    '''Read and overwrote different versions'''
    pass

class WriteSkew(AbortTransaction):
    '''Overwrote a version created after my snapshot'''
    
def make_db(tracker, stats, db_size, tid_watch, rid_watch,
            read_only_opt, safe_snaps,
            si_relax_writes, si_relax_reads, verbose, **extra_kwargs):
    '''P&G-style SSI observes that every serial dependency cycle under SI
    must include the following structure: T1 r:w T2 r:w T3, where T3
    commits first (Thm. 1 of P&G, citing Cahill's thesis).

    Based on that observation, we associate three timestamps with each
    version: s0, s1 and s2.

    Whenever a transaction T commits a write, it sets s0. This is the
    creation time of the version.

    Whenever transaction T performs a read, it checks whether s2 is
    set on any version. If so, T is doomed (being T1 of a dangerous
    structure where T3 committed first) and must abort. It next checks
    whether s1 is set. If so, T is the "pivot" and we prefer to abort
    it if T has overwritten any in-flight readers. If no such reader
    exists, then T is allowed to continue but it must remember the
    smallest s1 it encounters, in order to re-check overwritten
    readers during pre-commit. T also remembers the smallest s0 of
    any version it has read.

    At pre-commit, T checks again whether it is the "pivot" of any
    Dangerous Structure involving an in-flight T1, aborting if
    so. Otherwise, T stamps each version it created with s0 as its
    commit stamp, s1 as the remembered s0, and s2 as the remembered
    s1, if any was seen. There is no need to update in-flight readers
    because of the pivot test already performed.

    '''
    q,e = not verbose, errlog

    # interaction: read-only optimization only works under SI
    read_only_opt &= not si_relax_reads
    
    # read and write set
    tx_reads = collections.defaultdict(dict)
    tx_writes = collections.defaultdict(dict)
    
    # do we know we have an outbound r:w to committed T3? If so, keep
    # the oldest commit time of any such T3
    tx_have_t3 = {}

    # number of in-flight readers
    v_readers = collections.defaultdict(lambda:0)

    # (s1,s2) pair for each version
    v_stamps = {}

    # largest s0 of any committed reader
    v_rstamps = collections.defaultdict(lambda:0)

    safe_snapper = safe_snap_maker(tracker, safe_snaps)
    
    def tx_read(pid, rid, for_update=False):
        Q = q and pid not in tid_watch and rid not in rid_watch

        # already seen?
        dep = tx_reads[pid].get(rid, None)
        if dep is not None:
            return dep
            
        # take the newest version that precedes our snapshot.
        start = tracker.get_begin(pid)
        
        def read_filter(it):
            for x,dep,stamp in it:
                if si_relax_reads > 1 or pid == dep or stamp < start:
                    return x

            errlog('Oops! T=%d finds no suitable version of R=%d', pid, rid)
            errlog('\t(last version was %d, visible since %s, T started at %d)',
                   dep, stamp, start)
            assert not 'reachable'
                    
        dep,_ = yield from tracker.on_access(pid, rid, read_filter)

        Q or e('read: pid=%s rid=%s', pid, rid)

        # are we a doomed T1 in some Dangerous Structure?
        if not safe_snapper.get_snap(pid):
            try:
                v1,v2 = v_stamps.get((rid,dep), (None,None))
            except KeyError:
                assert not dep
                v1,v2 = None,None

            if v2:
                # read-only optimization: v2 is not a problem if we're
                # read-only and our snapshot precedes v2
                if not read_only_opt or pid in tx_writes or not (start < v2):
                    raise EagerDoomedT1

            # update s0/s1 for pre-commit
            if v1:
                ct3 = tx_have_t3.setdefault(pid, v1)
                if ct3 >= v1:
                    ct3 = tx_have_t3[pid] = v1

            # add to the read set
            tx_reads[pid][rid] = dep
            v_readers[rid,dep] += 1
        
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (dep, rid))
        return dep
        

    def tx_write(pid, rid):
        assert not safe_snapper.get_snap(pid)
        
        Q = q and pid not in tid_watch and rid not in rid_watch
        Q or e('write: pid=%s rid=%d', pid, rid)

        # already overwritten?
        if rid in tx_writes[pid]:
            return
            
        # do the write
        dep,_ = yield from tracker.on_access(pid, rid, False)

        # shouldn't be here if we wrote this already
        assert dep != pid
        
        # Did we read a different version than this? We can't know for
        # sure, because we don't actually track the full read set, but
        # we can at least check the reads we did record.
        rdep = tx_reads.get(pid, {}).get(rid, None)
        if not si_relax_writes and rdep and rdep != dep:
            raise ReadSkew
        
        # can I actually see the version I'm overwriting?
        if tracker.is_known(dep):
            dep_begin = tracker.get_begin(dep)
            dep_end = tracker.is_committed(dep)
            assert dep_end
        else:
            dep_begin = dep_end = tracker.get_safe_point()
            
        if tracker.get_begin(pid) < dep_end:
            raise WriteSkew

        # are we T2 in a Dangerous Structure, with T1 in-flight and T3
        # committed? If so, abort because we can retry more easily
        # than T1 (and hope T1 doesn't abort for other reasons!).
        if v_readers.get((rid,dep), 0) > 0 and pid in tx_have_t3:
            # read-only optimization: we're safe if T1 is read-only
            # (so far) and took its snapshot before T3 committed.
            if dep in tx_writes or not (dep_begin < tx_have_t3[pid]):
                raise EagerDoomedT2

        # are we T2 in a Dangerous Structure, where we started before
        # the last safe snapshot was taken? (T1 is the safe snap)
        last_snap = safe_snapper.get_last_snap()
        if not (last_snap < tx_have_t3.get(pid, last_snap+1)):
            raise SafeSnapKill

        # record the write
        tx_writes[pid][rid] = dep

        # TODO: safe to remove rid from read footprint?
        
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='%s=db[%s]' % (dep, rid))
        
    tid_watched = set()
    def tx_create(pid, read_only):
        if tid_watch and len(tid_watch) == len(tid_watched):
            hanging = set()
            for x in tid_watched:
                if tracker.is_known(x):
                    hanging.add(x)
            if hanging:
                errlog('Unfinalized pids: %s', ','.join(map(str, hanging)))
            yield from sys_exit(0)
            
        #t.begin = yield from sys_now()
        tracker.on_begin(pid)
        if read_only:
            yield from safe_snapper.request_safe_snap(pid)

        
    def tx_commit(pid):
        Q = q and pid not in tid_watch
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        Q or e('Commit %s', pid)
        if pid in tid_watch:
            tid_watched.add(pid)

        # get a serialization point, not reused even if we abort
        s0 = tracker.on_precommit(pid)
        
        # /// BEGIN CRITICAL SECTION ///

        if not safe_snapper.get_snap(pid):
            # No need to check whether we're T1 again. If T3 committed
            # first, then T2 knew about it (if it already committed), or
            # will know about both T3 and us (if we commit first).

            # check if we're T2 in a bad spot: any write that clobbered a
            # read of ours is Bad News if we clobbered any
            # readers... unless those readers all committed before any
            # T3. The timestamp of each T3-clobber is stored as s1 in the
            # versions we read, we want the earliest one.
            s1 = None
            reads = tx_reads.pop(pid, {})
            for rid,dep in reads.items():
                v1,_ = v_stamps.get((rid,dep), (None,None))
                if v1 and (not s1 or s1 > v1):
                    s1 = v1

                # release read lock before checking writes
                v_readers[rid,dep] -= 1

            # find the latest reader
            rstamp = 0
            writes = tx_writes.pop(pid, {})
            if s1:
                # did we tangle with a safe snapshot?
                last_snap = safe_snapper.get_last_snap()
                if not (last_snap < s1):
                    raise SafeSnapKill

                for rid,dep in writes.items():
                    rstamp = max(rstamp, v_rstamps[rid,dep])
                    if v_readers[rid,dep] or not (rstamp < s1):
                        raise PrecommitT2

            # stamp overwritten version for posterity
            for rid,dep in writes.items():
                v_stamps[rid,dep] = (s0,s1)

            # read-only optimization: if we committed read-only, record
            # our start time to reduce risk of false positives.
            if read_only_opt and not writes:
                s0 = tracker.get_begin(pid)

            # update rstamp now that we know we've committed
            for rid,dep in reads.items():
                rstamp = v_rstamps[rid,dep]
                if rstamp < s0:
                    rstamp = v_rstamps[rid,dep] = s0

        # /// END CRITICAL SECTION ///

        yield from tracker.on_finish(pid, True)
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        
    def tx_abort(pid):
        Q = q and pid not in tid_watch
        if pid in tid_watch:
            tid_watched.add(pid)

        tx_reads.pop(pid,None)
        tx_writes.pop(pid,None)
        tx_have_t3.pop(pid,None)
            
        Q or e('Abort %d', pid)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='red')
        yield from tracker.on_finish(pid, False)


    def fini():
        print_general_stats(stats)
        print_failure_causes(stats)


    return NamedTuple(db_size=db_size, tx_begin=tx_create,
                      tx_read=tx_read, tx_write=tx_write,
                      tx_commit=tx_commit, tx_abort=tx_abort,
                      fini=fini,
                      begin_tracking=tracker.begin_tracking,
                      end_tracking=tracker.end_tracking)
        
                


def test_ssi3_db():

    R,U,X = 1,2,3
    def test_fini(db):
        done = False
        def callback():
            nonlocal done
            done = True

        def nop():
            pid = yield from sys_getpid()
            yield from db.tx_begin(pid)
            yield from db.tx_abort(pid)

        yield from sys_sleep(1000*ONE_TICK)
        db.end_tracking(callback)
        yield from sys_spawn(nop())
        yield from sys_sleep(1000*ONE_TICK)
        yield from sys_spawn(nop())
        yield from sys_sleep(10000*ONE_TICK)
        assert done    
        db.fini()
        yield from sys_exit()
            

    def access(db, pid, rid, mode, delay):
        yield from sys_sleep(delay*ONE_TICK)
        yield from db.tx_write(pid, rid) if mode == X else db.tx_read(pid, rid, mode == U)

    def commit(db, pid, delay):
        if not isinstance(delay, int):
            errlog('bad delay: %s', delay)
            
        yield from sys_sleep(delay*ONE_TICK)
        yield from db.tx_commit(pid)
        
    def tx_one(db, rid, mode, delay1, delay2):
        def thunk():
            pid = yield from sys_getpid()
            try:
                yield from db.tx_begin(pid)
                yield from access(db, pid, rid, mode, delay1)
                yield from commit(db, pid, delay2)
            except AbortTransaction:
                yield from db.tx_abort(pid)
                
        return (yield from sys_spawn(thunk()))
        
    def tx_two(db, rid1, mode1, rid2=None, mode2=None, delay1=0, delay2=0, delay3=0):
        def thunk():
            pid = yield from sys_getpid()
            try:
                yield from db.tx_begin(pid)
                yield from access(db, pid, rid1, mode1, delay1)
                yield from access(db, pid, rid2 or rid1, mode2 or mode1, delay2)
                yield from commit(db, pid, delay3)
            except AbortTransaction:
                yield from db.tx_abort(pid)
                
        return (yield from sys_spawn(thunk()))

    def tx_n(db, commit_delay, *args):
        # accept (rid,mode,delay) triples
        def thunk():
            pid = yield from sys_getpid()
            try:
                yield from db.tx_begin(pid)
                for rid,mode,delay in args:
                    yield from access(db, pid, rid, mode, delay)
                yield from commit(db, pid, commit_delay)
            except AbortTransaction:
                yield from db.tx_abort(pid)
                
        return (yield from sys_spawn(thunk()))
        
    def test1(db):
        '''reads coexist peacefully'''
        # R-lock at t=0, S-locks at t=2 and t=2
        yield from tx_one(db, 1, R, 0, 10)
        yield from tx_one(db, 1, R, 1, 8)
        yield from tx_one(db, 1, R, 2, 6)
        yield from test_fini(db)

    def test2(db):
        '''incompatible requests block until the holder leaves'''
        yield from tx_one(db, 1, R, 0, 10)
        yield from tx_one(db, 1, X, 2, 0)
        yield from test_fini(db)
            
            
    def test3(db):
        '''reads can coexist with one upgrade lock, but a second upgrader
        blocks until the first leaves. Also make sure that the first
        upgrader can still upgrade when the lock mode is W.

        '''
        yield from tx_one(db, 1, R, 0, 10)
        yield from tx_two(db, rid1=1, mode1=U, mode2=X, delay1=2, delay2=8)
        yield from tx_two(db, rid1=1, mode1=U, mode2=X, delay1=3, delay2=6)
        yield from test_fini(db)
            

    def test4(db):
        '''new readers can coexist with an upgrade lock but not with an
        in-progress upgrade.

        '''
        yield from tx_one(db, 1, R, 0, 4)
        yield from tx_two(db, rid1=1, mode1=U, mode2=X, delay1=1, delay2=1)
        yield from tx_one(db, 1, R, 2, 4)
        yield from tx_one(db, 1, R, 4, 0)
        yield from test_fini(db)

    def test5(db):
        '''reader cannot upgrade if an upgrade lock has been granted'''
        yield from tx_two(db, rid1=1, mode1=R, mode2=X, delay2=5, delay3=1)
        yield from tx_one(db, 1, U, 1, 10)
        yield from test_fini(db)

    def test6(db):
        '''simple two-party deadlock detected'''
        yield from tx_two(db, rid1=1, mode1=R, rid2=2, mode2=X, delay2=5)
        yield from tx_two(db, rid1=2, mode1=R, rid2=1, mode2=X, delay1=1)
        yield from test_fini(db)

    def test7(db):
        '''three-party deadlock detected'''
        yield from tx_two(db, rid1=1, mode1=R, rid2=2, mode2=X, delay2=5)
        yield from tx_two(db, rid1=2, mode1=R, rid2=3, mode2=X, delay1=1)
        yield from tx_two(db, rid1=3, mode1=R, rid2=1, mode2=X, delay1=2)
        yield from test_fini(db)

    def test8(db):
        '''SI read-only anomaly detected:
	T1		T2		T3
			read A
			read B
					write B' (T2)
					D-commit (T2)
	read A
	read B' (T3)
	read C
	D-commit (T2 T3)
			write C' (T1)
			!! abort !!
					I-commit
	I-commit
        '''
        yield from tx_n(db, 0, (0, R, 200), (1, R, 0), (2, R, 0))
        yield from tx_n(db, 0, (0, R, 0), (1, R, 0), (2, X, 300))
        yield from tx_n(db, 0, (1, X, 100))
        yield from test_fini(db)

    A,B,C,D = 0,1,2,3

    def test9(db):
        '''Complex scenario that SI and 2PL both reject:
	T1		T2		T3		T4		T5
	read A
							write A' (T1)
							D-commit (T1)
					read B
    			write B' (T3)
			write C'
			D-commit (T3)
									write C'' (T2)
									D-commit (T2, T3)    
    	read C'' (T5)
					read C (**)
    					write D'
					DI-commit
			I-commit
									I-commit
	write D''
	DI-commit
							I-commit    
        
        '''
        yield from tx_n(db, 0, (A, R, 0), (C, R, 500), (D, X, 300))
        yield from tx_n(db, 0, (B, X, 300), (C, X, 0))
        yield from tx_n(db, 0, (B, R, 200), (C, R, 400), (D, X, 0))
        yield from tx_n(db, 0, (A, X, 100))
        yield from tx_n(db, 0, (C, X, 400))
        yield from test_fini(db)

    def test10(db):
        '''Schedule found during a measurement run. The Dangerous Structure
        has not yet formed when T2 commits first; when
        T3 commits second it sees T1 rw(A) T2 rw(B) T3:
        
	T1		T2		T3		
	                read B
	                		write B'
	                write A'
	read A          				
	                D-commit
	                                write C'
	                                D-commit
	read C'                         		
	D-commit	                                

        '''
        yield from tx_n(db, 100, (A, R, 0), (C, X, 200))
        yield from tx_n(db, 0,   (B, X, 100), (A, X, 400))
        yield from tx_n(db, 0,   (C, R, 300), (B, R, 300))
        yield from test_fini(db)

    def test11(db):
        '''Schedule found during a measurement run. The Dangerous Structure
        has not yet formed when T2 commits first; when
        T3 commits second it sees T1 rw(A) T2 rw(B) T3:
        
	T1		T2		T3
			read B
			write A'
					write C'
	read A
			commit
					write B'
	                                commit
	write C''
	commit

        '''
        yield from tx_n(db, 0, (A, R, 200), (C, X, 300))
        yield from tx_n(db, 300,   (B, R, 0), (A, X, 0))
        yield from tx_n(db, 0,   (C, X, 200), (B, X, 300))
        yield from test_fini(db)

    def test12(db):
        '''Schedule found during a measurement run. A dependency cycle forms
        without manifesting any Dangerous Structures:
        
	T1		T2		T3		T4
	                				read A
	write A'
	write B'
			read C
	D-commit
					write C'
	                                write D'
	                                D-commit
	                                		read D'
			read B'
	                				D-commit
			D-commit


        Adversarial commit ordering can make it even nastier. Here, T1
        is arguably the problem transaction, since its read of D' is
        impossible under SI, but it commits before T3 even reads
        C'. That means it's not enough for T1 to detect that it has
        gone awry; T3 must know to abort itself even though it has
        done nothing "wrong."
        
	T1		T2		T3		T4		
	read A                          				
	                write A'
	                write C'
	                D-commit
	                		read B
	                				write B'
	                                                write D'
	                                                D-commit
	read D'                                         		
	D-commit                        				
	                		read C'
					D-commit

        Note that there's still a shadow of the Dangerous Structure
        here. Every cycle must include (at least) two RW dependencies:
        one leaks information out of an uncommitted transaction and
        the other circumvents the commit-time dependency tracking that
        normally prevents the leakage from causing isolation
        failures. The RW deps just don't have to be adjacent any more
        after we give up SI.

        Thought: perhaps we can check D-commit times: it's a bad sign
        if I read a version that was clobbered before 1+ of the
        transactions in my dependency set. In the above example, T3
        depends on {T1 T2}, and gives a RW dep to T4; T4 D-committed
        before T1, and so T1 could potentially be poisoned by
        T4. There could be a lot of false positives, though: if T4 did
        not write D' at all (writing only B', for example), then there
        is no cycle. This looks suspiciously similar to wound-wait,
        which has a painfully high false positive rate.

        '''
        yield from tx_n(db, 300, (A, X, 0), (B, X, 0))
        yield from tx_n(db, 200, (C, R, 100), (B, R, 600))
        yield from tx_n(db, 0,   (C, X, 500), (D, X, 0))
        yield from tx_n(db, 200, (A, R, 0), (D, R, 600))
        yield from test_fini(db)

    def test13(db):
        '''One of the simplest possible serialization anomalies
        T1		T2
        read A
        		write A'
        		write B'
        		D-commit
        read B
        D-commit
        '''
        yield from tx_n(db, 0, (A, R, 0), (B, R, 300))
        yield from tx_n(db, 0, (A, X, 100), (B, X, 0))
        yield from test_fini(db)

    def test14(db):
        '''Scenario suggested by Alan Fekete:

	T1		T2		T3
					Write A'
			Read B
					Write B'
	Write C'
					Commit
	Read A'
	Commit
			Write C''
			Commit

        T2 should abort.
        '''
        yield from tx_n(db, 0, (C, X, 400), (A, R, 200))
        yield from tx_n(db, 0, (B, R, 100), (C, X, 600))
        yield from tx_n(db, 200, (A, X, 0), (B, X, 200))
        yield from test_fini(db)

    def test15(db):
        '''This scenario found during simulation run:

        T1		T2		T3		T4
        write A'
        write B'
        		read B
        D-commit
        				read C
        		write C'
        		D-commit
        						read D
        				write D'
        						read A'
        				D-commit
        						D-commit

        Produces the cycle T1 -- T4 rw T3 rw T2 rw T1 
        '''
        yield from tx_n(db, 200, (A, X, 0), (B, X, 0))
        yield from tx_n(db, 0, (B, R, 200), (C, X, 300))
        yield from tx_n(db, 200, (C, R, 400), (D, X, 400))
        yield from tx_n(db, 200, (D, R, 700), (A, R, 200))
        yield from test_fini(db)

    def test16(db):
        '''Scenario found during a simulation run. Requires Test #2 to avoid
        wedging the DB, even under SI. T4 should abort.

        T1		T2		T3		T4
        				read C
        				write D'
        read A
        write C'
        D-commit
        		write A'
        		write B
        		D-commit
        						read B'
        						read D
        				D-commit
        						D-commit

        T1 rw T2 -- T4 rw T3 rw T1

        '''
        yield from tx_n(db, 0, (A, R, 100), (C, X, 0))
        yield from tx_n(db, 0, (A, X, 400), (B, X, 0))
        yield from tx_n(db, 600, (C, R, 0), (D, X, 0))
        yield from tx_n(db, 200, (B, R, 600), (D, R, 0))
        yield from test_fini(db)

    failures = 0
    for test in (test1,test2,test3,test4,test5,test6,test7,test8,test9,
                 test10,test11,test12,test13,test14,test15,test16):
        errlog('\n%s\nRunning test:\n\n\t%s\n', '='*72, test.__doc__)
        stats = NamedTuple()
        tracker = dependency_tracker(stats)
        db = make_pg_ssi2_db(stats=stats, tracker=tracker,db_size=10, ww_blocks=True, verbose=True)
        db.begin_tracking()
        try:
            simulator(test(db), log=log_svg_task())
        except:
            traceback.print_exc()
            failures +=1
        
    if failures:
        exit(1)
    
        
        
    
    
if __name__ == '__main__':
    try:
        seed = sys.argv[1]
    except:
        seed = make_seed()

    errlog('Using RNG seed: %s', seed)
    random.seed(seed)

    test_ssi3_db()
    #simulator(make_benchmark(make_db=make_si_db, nclients=50, max_inflight=100, db_size=1000, duration=10000), log=log_svg_task())
