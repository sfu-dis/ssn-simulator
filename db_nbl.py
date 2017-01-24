import collections, random, sys, traceback
from dbsim import *

class UncommittedClobber(AbortTransaction):
    '''WW conflict with uncommitted write'''
    pass

class InvisibleClobber(AbortTransaction):
    '''WW conflict with committed-but-invisible write'''
    pass

class WaitDepth(AbortTransaction):
    '''Blocking on WW would exceed wait depth limit'''
    pass

class Test1Fail(AbortTransaction):
    '''T1 rw T rw T2 and T1 does not precede T2's timestamp'''
    pass
    
class Test2Fail(AbortTransaction):
    '''T1 -- T rw T2 and T1 does not precede T2's timestamp'''
    pass

class DBWedged(Exception):
    '''A cycle of D-committed transactions has been detected.

    NOT a type of AbortTransaction; none of the victims can abort.
    '''
    pass
    
def make_db(tracker, stats, nrec, tid_watch, rid_watch,
            verbose, **extra_kwargs):
    '''A database model based on blacklist isolation (BI) and generalized
cycle detection

    '''


    class Transaction(object):
        def __init__(self, tid):
            self.tid = tid
            self.deps, self.war, self.depstar = set(),set(),None
            self.reads, self.clobbers = {}, {}
            self.dcommit = self.icommit = self.durable = 0
            self.last_write,self.blocked = None,False
            self.stamp = None

    v_readers = collections.defaultdict(set)

    zerotx = Transaction(0)
    zerotx.dcommit = zerotx.icommit = zerotx.dcommit = 1
    zerotx.depstar = frozenset()
    in_flight = {0:zerotx}
    
    q,e = not verbose, errlog

    def tx_read(pid, rid, for_update=False):
        Q = q and pid not in tid_watch and rid not in rid_watch

        t = in_flight[pid]
        Q or e('read: pid=%s rid=%s', pid, rid)
        # have I seen this record before?
        dep = t.reads.get(rid, None) or (rid in t.clobbers and pid)
        if dep:
            return dep

        def read_filter(it):
            for i,xmin,_ in it:
                x = in_flight[xmin]
                if x.icommit:
                    # definitely safe to use
                    '''NOTE: I-commit occurs the instant t.icommit becomes
                    True. Afterward, t can set v.t = zerotx for all v it
                    created, allowing t to be deleted.

                    '''
                    Q or e('\tUsing I-committed version %s of rid %s', x.tid, rid)
                    return i

                if x.dcommit:
                    # safe unless we're in the tid's WAR-set
                    if t not in x.depstar:
                        Q or e('\tUsing visible D-committed version %s of rid %s', x.tid, rid)
                        return i
                    q or e('\tSkipping invisible D-committed version: pid=%s', x.tid)
            else:
                assert not 'reachable'

        dep,_ = yield from tracker.on_access(pid, rid, read_filter)
        
        v_readers[rid,dep].add(t)
        q or e('\tNew v.r: %s', ' '.join(map(str, (d.tid for d in v_readers[rid,dep]))))

        clobber,_ = tracker.get_overwriter(rid, dep, False)
        if clobber is not None:
            X = Q and clobber not in tid_watch
            clobber = in_flight[clobber]
            clobber.war.add(t)
            X or e('\tNew WAR for %s via rid %d: %s', clobber.tid, rid,
                   ' '.join(map(str, (d.tid for d in clobber.war))))

        t.reads[rid] = dep
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (dep, rid))
        return dep
        

    def tx_write(pid, rid):
        Q = q and pid not in tid_watch and rid not in rid_watch
        t = in_flight[pid]
        Q or e('write: pid=%s rid=%d', pid, rid)
        
        # have I written this before?
        if rid in t.clobbers:
            q or e('\talready wrote to this record')
            return

        # do the write (don't depend on the version stamp)
        dep,_ = yield from tracker.on_access(pid, rid, False)

        x = in_flight[dep]
        assert x is not t # can't clobber a version we created
        if x.icommit:
            # definitely safe to clobber
            n = len(v_readers.get((rid,dep), ()))
            q or e('\tClobbering I-committed version with pid=%s and %d reader(s)', x.tid, n)
            pass
        elif x.dcommit:
            # only clobber versions we can actually see
            if t in x.depstar:
                q or e('\tAbort: cannot see latest version from pid=%s', x.tid)
                raise InvisibleClobber
                
            q or e('\tClobbering I-committed version: pid=%s', x.tid)
                
        # tie in my WAR and replace the current version
        v_readers[rid,dep].discard(t) # in case we previously read it
        t.reads.pop(rid, None)

        t.clobbers[rid] = dep
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='%s=db[%s]' % (dep, rid))

    tid_watched = set(tid_watch or [0])
    def tx_create(pid, is_readonly):
        if not tid_watched:
            yield from sys_exit(0)
            
        in_flight[pid] = t = Transaction(pid)
        t.begin = yield from sys_now()
        tracker.on_begin(pid)
        
    def finish(pid):
        t = in_flight[pid]
        then,now = t.begin, (yield from sys_now())
        histo_add(resp_histo, then-now)

    def i_commit(t):
        Q = q and t.tid not in tid_watch
        Q or e('\tI-commit %d', t.tid)
        assert not t.deps
        assert not t.war
        t.icommit = tracker.get_next_stamp()

        # clear dependencies
        commits = set()
        for rid,dep in t.clobbers.items():
            R = Q and rid not in rid_watch
            # I created this version; delete its predecessor
            R or e('\tFinalize version %d of rid=%d', t.tid, rid)

            x,_ = tracker.get_overwriter(rid, dep, False)
            x = in_flight.get(x, None)
            if x and x.deps:
                X = Q and x.tid not in tid_watch
                X or e('\tpid %d I-commit no longer blocked on %d', x.tid, t.tid)
                x.deps.discard(t)
                if not x.deps and not x.war and x.dcommit and not x.icommit:
                    assert not x.icommit
                    assert x not in commits
                    commits.add(x)

                X or e('\t=> remaining deps={%s} war={%s}', 
                       ' '.join(map(str, (d.tid for d in (x.deps or ())))),
                       ' '.join(map(str, (d.tid for d in x.war))))

            for x in v_readers[rid,t.tid]:
                X = Q and x.tid not in tid_watch
                # if x accessed a version I created, I ended up in
                # x.deps. If x then committed, I also ended up in
                # x.war; I may also be in x.war due to it
                # clobbering some version I read, but there's no
                # harm removing myself now (the read will be
                # removed soon enough).
                X or e('\tpid %d I-commit no longer blocked on %d', x.tid, t.tid)
                if x.deps:
                    x.deps.discard(t)
                    if not x.deps and not x.war and x.dcommit and not x.icommit:
                        assert not x.icommit
                        assert x not in commits
                        commits.add(x)

                X or e('\t=> remaining deps={%s} war={%s}', 
                       ' '.join(map(str, (d.tid for d in (x.deps or ())))),
                       ' '.join(map(str, (d.tid for d in x.war))))

        for rid,dep in t.reads.items():
            R = Q and rid not in rid_watch
            # remove myself from the version's read set
            Q or e('\tRemove %d from read set of rid=%d', t.tid, rid)
            v_readers[rid,dep].remove(t)

            x,_ = tracker.get_overwriter(rid,dep,False)
            x = in_flight.get(x, None)
            if x and x.war:
                X = Q and x.tid not in tid_watch
                # my read no longer prevents x from I-committing its clobber of v
                x.war.discard(t)
                X or e('\tpid %d I-commit no longer blocked on WAR %d', x.tid, t.tid)
                X or e('\t=> remaining WAR deps: %s', 
                       ' '.join(map(str, (d.tid for d in x.war))))
                if not x.war and not x.deps and x.dcommit and not x.icommit:
                    assert not x.icommit
                    #bad assertion: new versions could arrive after I D-commit
                    #assert v is db[rid].prev
                    assert x not in commits
                    commits.add(x)

                X or e('\trid=%s pid=%d dcommit=%s WAR={%s} deps={%s}',
                       rid, x.tid, x.dcommit,
                       ' '.join(map(str, (d.tid for d in x.war))),
                       ' '.join(map(str, (d.tid for d in x.deps))))
            
        for x in commits:
            i_commit(x)

        
        if t.durable:
            tracker.on_finalize(t.tid)

    def tx_commit(pid):
        Q = q and pid not in tid_watch
        t = in_flight[pid]
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        Q or e('Commit %s', pid)
        tid_watched.discard(pid)

        # /// BEGIN CRITICAL SECTION ///

        # construct WAR set (as of D-commit); we'll install it at all
        # clobbered versions after the commit succeeds.
        assert not t.war
        t3_list = set()
        for rid,dep in t.clobbers.items():
            x = in_flight[dep]
            if not x.icommit:
                # still not I-committed
                t.deps.add(x)

            t.war |= v_readers[rid,dep]
                
        for rid,dep in t.reads.items():
            assert t in v_readers[rid,dep]
            x = in_flight[dep]
            if not x.icommit:
                t.deps.add(x)
            x,_ = tracker.get_overwriter(rid,dep,False)
            x = in_flight.get(x, None)
            if x:
                t3_list.add(x)
            
        ds = set(t.war)
        
        # cycle test required?
        if t3_list:
            q or e('\tt3_list={%s}',
                   ' '.join('%s@%s/%s' % (x.tid,x.dcommit,x.stamp) for x in t.deps))
            t3_min = min(x.stamp for x in t3_list)
            if t.war:
                '''Test #1
                
                Abort T if T1 rw T rw T2 exists where T1 and T2 are
                already committed and T1 did not commit before T2's
                timestamp.

                '''
                t1_max = max(x.dcommit for x in t.war)
                if not (t1_max < t3_min):
                    raise Test1Fail
            if t.deps:
                '''Test #2
                
                Abort T if T1 -- T rw T2 exists where T1 and T2 are
                already committed and T1 did not commit before T2's
                timestamp

                '''
                q or e('\tApply test #2 to %d', pid)
                q or e('\tdeps={%s}',
                       ' '.join('%s@%s/%s' % (x.tid,x.dcommit,x.stamp) for x in t3_list))
                t1_max = max(x.dcommit for x in t.deps)
                if not (t1_max < t3_min):
                    raise Test2Fail

            assert t3_min
            t.stamp = t3_min
            
        for x in t.deps:
            X = Q and x.tid not in tid_watch
            X or e('\tpid %s has I-commit dep on %s (d-commit:%s i-commit:%s)',
                   pid, x.tid, x.dcommit, x.icommit)
            try:
                assert not x.icommit
            except AssertionError:
                errlog('pid %s has stale I-commit dep on %s', pid, x.tid)
                raise
            ds.add(x)
            ds.update(d for d in x.depstar if not d.icommit)
            
        if ds:
            Q or e('\tdepstar for %d at D-commit: %s', pid,
                   ' '.join(map(str, (d.tid for d in ds))))
        
        t.dcommit = tracker.on_precommit(pid) # writes now visible to non-dependents
        if not t.stamp:
            t.stamp = t.dcommit
            
        t.depstar = ds
        # /// END CRITICAL SECTION ///
            
        for x in t.war:
            assert not x.icommit
            assert x.tid in in_flight
        for x in t.deps:
            assert not x.icommit
            assert x.tid in in_flight
        if not t.war and not t.deps:
            i_commit(t)
            
        yield from tracker.on_finish(pid, True)
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        
    def tx_abort(pid):
        Q = q and pid not in tid_watch
        t = in_flight[pid]
        Q or e('Abort %d', pid)

        commits = set()
        for rid,dep in t.clobbers.items():
            R = Q and rid not in rid_watch
            # I created this version, delete it
            R or e('\tRoll back update of rid=%d', rid)

            
        for rid,dep in t.reads.items():
            R = Q and rid not in rid_watch
            R or e('\tRemove %d from read set of rid=%d', t.tid, rid)
            v_readers[rid,dep].remove(t)

            x,_ = tracker.get_overwriter(rid,dep,False)
            x = in_flight.get(x,None)
            if x and x.war:
                X = Q and x.tid not in tid_watch
                x.war.discard(t)
                X or e('\tpid %d I-commit no longer blocked on WAR %d', x.tid, pid)
                X or e('\t=> remaining WAR deps: %s', 
                       ' '.join(map(str, (d.tid for d in x.war))))
                if not x.war and not x.deps and x.dcommit and not x.icommit:
                    assert not x.icommit
                    assert x not in commits
                    commits.add(x)
                else:
                    q or e('\trid=%s still has readers waiting to I-commit: %s',
                           rid, ' '.join(map(str, (d.tid for d in x.war))))
            elif x:
                q or e('\tskipping pid=%d with empty WAR', x.tid)

        t.dcommit = t.icommit = t.durable = yield from tracker.on_finish(pid, False)
        del in_flight[t.tid]

        for x in commits:
            i_commit(x)
                                  
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='red')


    def fini():
        live = [t for t in in_flight.values() if not t.icommit]
        if live:
            errlog('\nFound %d live transactions at exit (oldest from tick %.2f):',
                   len(live), min(t.begin for t in live)/float(ONE_TICK))
            if not q:
                for t in live:
                    errlog('\tpid=%s deps={%s}', t.tid, ' '.join(map(str, (x.tid for x in t.war))))

        print_general_stats(stats)
        print_failure_causes(stats)


    return NamedTuple(nrec=nrec, tx_begin=tx_create,
                      tx_read=tx_read, tx_write=tx_write,
                      tx_commit=tx_commit, tx_abort=tx_abort,
                      fini=fini,
                      begin_tracking=tracker.begin_tracking,
                      end_tracking=tracker.end_tracking)
        
                


def test_nbl_db():

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

    failures = 0
    for test in (test1,test2,test3,test4,test5,test6,test7,test8,test9,
                 test10,test11,test12,test13,test14,test15):
        errlog('\n%s\nRunning test:\n\n\t%s\n', '='*72, test.__doc__)
        stats = NamedTuple()
        tracker = dependency_tracker(stats)
        db = make_nbl_db(stats=stats, tracker=tracker,nrec=10, ww_blocks=True, verbose=True)
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

    test_nbl_db()
    #simulator(make_benchmark(make_db=make_si_db, nclients=50, max_inflight=100, nrec=1000, duration=10000), log=log_svg_task())
