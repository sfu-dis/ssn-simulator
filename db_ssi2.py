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

def make_ssi2_db(tracker, stats, nrec, tid_watch=(), rid_watch=(),
                danger_check=2, ww_blocks=False, verbose=False, **extra_kwargs):
    '''A database record is comprised of one or more versions. New readers
    always use the most recently committed version, even if a write is
    in progress, and so RAW and WAR conflicts are nonblocking; writers
    do block writers when WAW conflicts arise.

    Because we allow WAR hazards, we risk serialization failures:

	T1		T2		T3
	read A    
			write A'
					read A
			write B'
			commit    
	read B'
					read B'

    We can enforce a serial schedule by forbidding T2 to commit A'
    until all readers using A have finished, with A' and B' remaining
    invisible during the wait:
    
	T1		T2		T3
	read A    
			write A'
					read A
			write B'
			D-commit
	read B
	DI-commit
					read B
					DI-commit
			I-commit

    Note that T2 is not in doubt during the wait. The transaction has
    committed and can release results to the user once its commit
    record is durable. We merely prevent the changes from becoming
    visible to other transactions until all dependent transactions
    have completed. We therefore start to distinguish between D-commit
    (writing a commit log that signals the transaction as no longer in
    doubt) and I-commit (making changes visible to other transactions). 

    Unfortunately, the above policy allows readers to block writers
    indirectly: a reader who forces a committed transaction's changes
    to remain invisible also prevents the next writer from
    proceeding. To avoid that bad outcome, we allow T2's results to
    become mostly-visible between D-commit and I-commit: everyone can
    see them *excepting* those transactions which delay T2's I-commit
    (T1 and T3 in this case). Note that this policy is *not* snapshot
    isolation, as the following schedule demonstrates:
    
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

    Under our scheme, no transaction blocks and all five commit. C'
    and C'' are invisible to T3 (in spite of T2 and T5 having
    committed), and so T3 reads C instead. The final serial order is
    {T3 T2 T5 T1 T4}.

    Under SI, T1's read of A would force it to also read B, placing it
    before T2 and T3 in the serial order. The attempt to write D''
    would then trigger an abort in conflict with T3's write to D',
    which would have already become visible by then.

    Note that, just as with SI, a transaction's chances of aborting on
    write increase with the time elapsed since its first read. We can
    always serve reads from an older version to preserve isolation,
    but we must not allow mutually invisible writes to coexist. For
    example, T3 would abort if it attempted to write C, because its
    read of B forces us to isolate it from T2.

    Returning to the classic read-only anomaly for SI, we preserve
    serial behavior, without blocking, by aborting T2:

	T1		T2		T3
			read A
			read B
					write B' (T2)
					D-commit (T2)
	read A
	read B' (T3)
	D-commit (T2 T3)
			write A' (T1)
			!! abort !!
					I-commit
	I-commit

    2PL would preserve serializability without aborting (T2 < T3 <
    T1), but would allow T2 to hold up the works. This is highly
    unfortunate, because T2 is read-only at the time it prevents T1
    (also read-only) from proceeding. That's why 2PL blows up under
    contention: it allows readers to block other readers.

    Our scheme also catches a similar, more subtle, SI anomaly:

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

    Here, we should have T1 < T2 < T3, because T2 does not see B'
    (from T3) and T1 does not see C' (from T2). However, T1 cannot
    precede T3 because it has seen B' (which T3 wrote).

    Two nice properties of this setup:

    1. Transactions acquire dependencies on other transactions, not on
    other versions. Versions are merely dependency carriers. There are
    vastly fewer active transactions than records in the system, which
    is helpful if a long-running reader forces us to store
    dependencies for a long time.

    2. Transaction Ti can only acquire a WAW or RAW dependency on
    transactions Tj *after* Tj has committed and finalized its
    dependency set. By construction, any transaction that aborts could
    only have conferred WAR dependencies, which aborts and commits
    resolve equally well. Ti can therefore compute an accurate
    dependency set, at commit time, as the union of its own direct
    dependencies and their respective dependency sets. Even better,
    some or all of those direct dependencies may have already cleared
    by the time Ti commits, allowing us to bypass the indirect ones.

    Every record maintains the set of active readers; in the absence
    of a writer, readers add themselves to the set on arrival and
    remove themselves on commit/abort. When a writer arrives, it
    unions the active read set with its own WAR set. From that point
    on, all future readers update both sets on arrival and
    departure. We keep the record's own read set around in case the
    writer aborts, to avoid polluting a future writer with unrelated
    readers. When the writer makes a new version available, it creates
    a new read set to go with it, and the process repeats. As with SI,
    we can drop a version once it has a successor *and* all
    transactions that co-existed with it have finished.

    Any version with an I-committed successor can be removed once its
    read set becomes empty. By way of contradiction, suppose that a
    new reader TR could arrive after an I-committed successor (due to
    TU) appeared and the read set became empty. TR would only have
    ignored the newer version if TU carried a WAR dependency on TR. In
    that case, however, TU should yet have I-committed because TR has
    not finished. We conclude that such a TR cannot exist. Further, TU
    is guaranteed to I-commit once all readers in its WAR set have
    committed. Therefore, the last reader to leave a given read set
    can safely delete it the writer's WAR set was empty afterward
    (recall that we keep a pointer to the WAR set in each version). In
    case TR read several records written by TU, TU will I-commit after
    TR releases the first version in its read set, and TR will find
    the TU's WAR set empty when it releases the remaining versions.


    Each version contains a value, a read count, a TID stamp, and a
    pointer to the next newer verison. When a writer arrives, it
    allocates a new version, stamped with a negated TID, with read=0
    and next=NULL. It then follows the chain of next pointers until a
    successful compare-and-swap appends it to the chain. If it becomes
    the first writer in the chain, it sets its TID to positive and can
    make modifications. Otherwise, it leaves its TID negative and
    blocks. When an active writer commits, it does the following:

    1. Set the active version's TID to zero, signalling new readers
    that they may need to use the new version instead.

    2. Log the commit record.

    3. Mark the transaction as committed in the commit table.

    4. Use an atomic CAS to clear the active version's TID, signalling
    new readers that they need to use the new version instead. If a
    reader arrives after marking the transaction as committed and
    before clearing the TID, we squint and say it arrived before the
    transaction committed; we can get away with it because we're not
    using snapshot isolation.
    
    4. Wake the next writer, if any (to avoid races, always send a
    wakeup signal if the next pointer is non-NULL and the successor's
    TID is negative).

    5. If there was no next pointer, use an atomic CAS to install the
    new version (may fail if a writer arrives and races). If there was
    a next pointer, the next writer will install the new version using
    a normal store.

    When a reader arrives, it does the following:

    1. If the active version's TID is non-zero, use it; otherwise
    continue to next step.

    2. If the next version's TID is marked as committed, and the TID
    is still non-zero after checking, use that version; otherwise
    repeat this step until a valid version has been obtained.

    The TID and read count are embedded in the same 64-bit word, and
    readers use atomic CAS to increment the read count while verifying
    that the TID remains non-zero.

    checks for non-NULL next pointer; if found, it
    sends a wake-up signal to the next version's owner and does not attempt to activate the just-committed version it created (he new owner can do it non-atomically)will update the list head pointer to point at its predecessor
    
    Whenever a new reader arrives, it increments
    the read count and accesses the value, decrementing the count
    after it completes. When a writer arrives, it uses an atomic CAS
    to install a new version containing its TID, locking the
    record. Note that readers do not care whether a writer is present,
    but continue using the existing version. The writer, meanwhile,
    constructs a new versionperforms its update out of place. At some
    point before it commits, it must make the new version accessible
    in a special shadow table; after commit, readers can access the
    shadow entry

    in the e and
    can then update the record. The update is performed out of place,
    with the new value stored in a special shadow table for
    unfinalized versions. Readers who arrive after the writer claims
    the record but before it commits still see the original version,
    effectively placing them before the writer in the global serial
    order, and the writer is not allowed to complete until all
    existing readers have left. New readers that arrive after the
    writer begins the commit process will see the writer's update and
    in turn cannot commit until it finishes.

    The writer begins the commit process atomically by updating its
    entry in the transaction completion table; anyone who sees the TID
    will check the completion table, see that it has completed, and
    attempt to install the shadow version (the completing transaction
    is also trying to install the new version).

    Readers who re-read a value always use the version they originally
    read, even if later versions became available in the
    meantime. Unlike with snapshot isolation, where a query's start
    time determines which version it sees, here a reader sees the
    latest committed version on its first read, and repeated reads
    always come back to the same version. Once the last reader of a
    version leaves (decrementing the read count to zero), the version
    can be deleted. At the time of this deletion there may very well
    exist some older readers who would have seen the version, but did
    not actually read it during its lifetime.

    '''
    '''We track several kinds of dependency sets:

    t.deps is the set of all RAW and WAW dependencies t has acquired
    directly; t cannot I-commit until t.deps is empty, and t's depstar
    will include the union of their depstars in t.deps at D-commit.
    
    t.war holds all WAR dependencies t has picked up; it is installed
    as v.war at each version v which t clobbers, ensuring that readers
    who arrive later are tracked properly. Unlike RAW and WAW, WAR
    does not confer any indirect dependencies on t.

    v.r holds the current set of readers for version v. These readers
    confer WAR dependencies on any writer that clobbers the version.
    
    t.depstar is the transitive closure of t's dependencies, the set
    of transactions that must remain isolated from t even after it
    d-commits. Allowing any x in t.ideps to see t's updates would
    introduce a serialization anomaly. This set is defined as t.war |
    t.deps | {x.depstar : x in t.deps}, and its membership is fixed
    once t D-commits.

    '''
    db = collections.defaultdict(collections.deque)

    next_stamp = 1
    def get_next_stamp():
        nonlocal next_stamp
        rval,next_stamp = next_stamp,next_stamp+1
        return rval

    empty_set = frozenset()
    resp_histo = collections.defaultdict(lambda:0)
    in_flight = {}

    class Transaction(object):
        def __init__(self, tid):
            self.tid = tid
            self.deps, self.war = set(),set()
            self.footprint, self.clobbers = {}, {}
            self.dcommit = self.icommit = self.durable = 0
            self.last_write,self.blocked = None,False
            self.stamp = None

    class Version(object):
        def __init__(self, t, prev):
            self.t = t
            self.r, self.clobber = set(),None
            self.prev = prev

        def __iter__(self):
            v = self
            while v:
                yield v
                v = v.prev

    zerotx = Transaction(0)
    zerotx.dcommit = zerotx.icommit = zerotx.dcommit = get_next_stamp()
    
    dummy_version = Version(zerotx, None)
    db = collections.defaultdict(lambda: Version(zerotx, None))
    q,e = not verbose, errlog

    def histo_add(h, delay):
        h[delay.bit_length()] += 1

    def histo_print(h, title, xtime=True, logx=True, logy=True):
        b = 1./ONE_TICK if xtime else 1
        errlog('\n\tLog-log distribution of %s:', title)
        fmt = '\t\t%8.2f: %5d %s' if xtime else '\t\t%8d: %5d %s'
        for k in sorted(h.keys()):
            n = h[k]
            x = b*(2.**k if logx else k) if k else 0
            y = n.bit_length() if logy else n
            errlog(fmt, x, n, '*'*y)

    tx_snaps = collections.defaultdict(get_next_stamp)
    def tx_read(pid, rid, for_update=False):
        Q = q and pid not in tid_watch and rid not in rid_watch
        t = in_flight[pid]
        Q or e('read: pid=%s rid=%s', pid, rid)
        try:
            # have I seen this record before?
            v = t.footprint[rid]
            return v.t.tid
        except KeyError:
            pass # keep going...

        snap = tx_snaps[pid]
        q or e('\tNew record, check versions')
        assert not db[rid].clobber
        for v in db[rid]:
            x = v.t
            if x.dcommit < snap:
                # safe to use
                Q or e('\tUsing version %s of rid %s', x.tid, rid)
                break
        else:
            assert not 'reachable'

        if v.r is not None:
            v.r.add(t)
            q or e('\tNew v.r: %s', ' '.join(map(str, (d.tid for d in v.r))))

        if v.clobber is not None:
            X = Q and v.clobber.tid not in tid_watch
            v.clobber.war.add(t)
            X or e('\tNew WAR for %s via rid %d: %s', v.clobber.tid, rid,
                   ' '.join(map(str, (d.tid for d in v.clobber.war))))

        assert x is v.t
        t.footprint[rid] = v
        val = tracker.on_access(pid, rid, x.tid, True)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (val, rid))
        return val
        

    clobbering = {}
    no_clobber = (None,None)
    def tx_write(pid, rid):
        snap = tx_snaps[pid]
        Q = q and pid not in tid_watch and rid not in rid_watch
        t = in_flight[pid]
        Q or e('write: pid=%s rid=%d', pid, rid)
        
        # have I written this before?
        if rid in t.clobbers:
            q or e('\talready wrote to this record')
            return

        if ww_blocks:
            c,w = clobbering.get(rid,no_clobber)
            if c:
                if c.blocked or w:
                    raise WaitDepth
                clobbering[rid] = (c,t)
                Q or e('\tpid=%d blocked on WW conflict with pid=%d', pid, c.tid)
                t.blocked = True
                yield from sys_park()
            else:
                Q or e('\tpid=%d taking empty clobber slot for rid=%d', pid, rid)
                clobbering[rid] = (t,None)
                
            assert clobbering[rid][0] is t
            t.last_write = rid
            
        # writes demand latest version or bust (unlike reads)
        # TODO: attempt to block on write?
        v = db[rid]
        assert not v.clobber

        x = v.t
        assert x is not t # can't clobber a version we created
        if x.dcommit < snap:
            # definitely safe to clobber
            n = v.r and len(v.r) or 0
            Q or e('\tClobbering version %s with %d reader(s)', x.tid, n)
        else:
            # only clobber versions we can actually see
            raise InvisibleClobber
                
        # tie in my WAR and replace the current version
        assert v.r is not None
        v.r.discard(t) # in case we previously read it
        v = Version(t, v)

        t.last_write = None
        t.footprint[rid] = t.clobbers[rid] = v
        val = tracker.on_access(pid, rid, x.tid, False)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='%s=db[%s]' % (val, rid))

    def fail_if_wedged():
        earliest = min(in_flight)
        scc = check_for_cycles(earliest, 'Cycle check')
        if scc and not sum(1 for tid in scc if not in_flight[tid].dcommit):
            errlog('\n!!! Database wedged !!!')
            errlog('Transactions: %s\n!!!                 !!!\n', ' '.join(map(str, sorted(scc))))
            raise DBWedged
        
    tid_watched = set()
    def tx_create(pid):
        if not (pid % 300):
            not in_flight or fail_if_wedged()
            #errlog(' '.join(map(str,sorted(in_flight))))
                
        if tid_watch and len(tid_watch) == len(tid_watched):
            hanging = set()
            for x in tid_watched:
                if not x.icommit:
                    hanging.add(x.tid)
                    hanging.update(d.tid for d in x.deps)
                    hanging.update(d.tid for d in x.war)
                    errlog('At exit: watched pid=%s deps=%s war=%s', x.tid,
                           ','.join(map(str, (d.tid for d in (x.deps or ())))),
                           ','.join(map(str, (d.tid for d in (x.war or ())))))
            if hanging:
                errlog('Hanging pids: %s', ','.join(map(str, hanging)))
            yield from sys_exit(0)
            
        in_flight[pid] = t = Transaction(pid)
        t.begin = yield from sys_now()
        tracker.on_start(pid)
        
    def finish(pid):
        then,now = t.begin, (yield from sys_now())
        histo_add(resp_histo, then-now)

    def i_commit(t):
        Q = q and t.tid not in tid_watch
        Q or e('\tI-commit %d', t.tid)
        assert not t.deps
        assert not t.war
        t.icommit = get_next_stamp()

        # clear dependencies
        commits = set()
        for rid,v in t.footprint.items():
            R = Q and rid not in rid_watch
            if v.t is t:
                # I created this version; delete its predecessor
                R or e('\tFinalize version %d of rid=%d', t.tid, rid)
                assert v.prev.clobber is t
                if v.prev.r:
                    Q or e('\t--> unlinking previous version pid=%d with readers: %s',
                           v.prev.t.tid, ' '.join(map(str, (x.tid for x in v.prev.r))))
                    
                #bad assertion: v.prev.clobber can D-commit with readers
                #assert not v.prev.r
                
                #v.prev = None
                #v.t = zerotx

                x = v.clobber
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
                    
                for x in v.r:
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

            else:
                # remove myself from the version's read set
                Q or e('\tRemove %d from read set of rid=%d', t.tid, rid)
                v.r.remove(t)

                x = v.clobber
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
            
        del in_flight[t.tid]

        for x in commits:
            i_commit(x)

        
        if t.durable:
            tracker.on_finalize(t.tid)
        
    def check_for_cycles(pid, name, verbose=False):
        deps = collections.defaultdict(dict)
        for x in in_flight.values():
            for rid,v in x.footprint.items():
                if v.t is x:
                    deps[x.tid].setdefault(v.prev.t.tid, ('ww', rid))
                    for d in v.prev.r:
                        deps[x.tid].setdefault(d.tid, ('rw', rid))
                else:
                    deps[x.tid].setdefault(v.t.tid, ('wr', rid))

        dstring = lambda dcommit: dcommit and ('@%s' % dcommit) or ''
        
        scc = tarjan_incycle(deps, pid)
        if scc:
            scc = set(scc)
        if verbose and scc:
            errlog('\t=> %s dependency cycle contains:', name)
            edges = ['%s%s %s(%s) %s%s' % (tid,dstring(in_flight[tid].dcommit),
                                           dtype,rid,dep,
                                           dstring(in_flight[dep].dcommit))
                     for tid in scc
                     for dep,(dtype,rid) in deps[tid].items()
                     if dep in scc]
            errlog('\t\t%s' % '\n\t\t'.join(sorted(edges)))
            
        return scc
        
    def tx_commit(pid):
        Q = q and pid not in tid_watch
        t = in_flight[pid]
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        Q or e('Commit %s', pid)
        if pid in tid_watch:
            tid_watched.add(t)

        # /// BEGIN CRITICAL SECTION ///

        # construct WAR set (as of D-commit); we'll install it at all
        # clobbered versions after the commit succeeds.
        assert not t.war
        t3_list = set()
        for v in t.footprint.values():
            if v.t is t:
                if v.prev.clobber:
                    # raced with another writer and lost
                    raise UncommittedClobber
                if not v.prev.t.icommit:
                    # still not I-committed
                    t.deps.add(v.prev.t)

                t.war |= v.prev.r
            else:
                assert t in v.r
                if not v.t.icommit:
                    t.deps.add(v.t)
                if v.clobber:
                    t3_list.add(v.clobber)
            
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

        # install clobbered versions
        for rid,v in t.clobbers.items():
            v.prev.clobber = t
            db[rid] = v
            
        t.dcommit = get_next_stamp() # writes now visible to non-dependents
        if not t.stamp:
            t.stamp = t.dcommit
            
        # /// END CRITICAL SECTION ///

        # save space: clobbered versions no longer need read set
        #for rid,v in t.clobbers.items():
        #    v.prev.r = None
            
        for x in t.war:
            assert not x.icommit
            assert x.tid in in_flight
        for x in t.deps:
            assert not x.icommit
            assert x.tid in in_flight
        if not t.war and not t.deps:
            i_commit(t)
        else:
            Q or e('\tI-commit blocked on war={%s} deps={%s}',
                   ' '.join(map(str, (d.tid for d in t.war))),
                   ' '.join(map(str, (d.tid for d in t.deps))))
                   
            
        tracker.on_finish(pid, True)
        
        if ww_blocks:
            for rid in t.clobbers:
                c,w = clobbering.pop(rid)
                assert c is t
                if w:
                    X = Q and w.tid not in tid_watch
                    X or e('\tUnblocking pid=%s from rid=%s', w.tid, rid)
                    clobbering[rid] = (w,None)
                    w.blocked = False
                    yield from sys_unpark(w.tid)
                else:
                    Q or e('\tNobody waiting on rid=%d', rid)
            
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        t.durable = True
        if t.icommit:
            tracker.on_finalize(pid)
        
    def tx_abort(pid):
        Q = q and pid not in tid_watch
        t = in_flight[pid]
        if pid in tid_watch:
            tid_watched.add(t)
        Q or e('Abort %d', pid)

        commits = set()
        for rid,v in t.footprint.items():
            R = Q and rid not in rid_watch
            if v.t is t:
                # I created this version, delete it
                R or e('\tRoll back update of rid=%d', rid)

                # nobody else can see this version
                assert not v.clobber and not v.r
                assert t not in v.r
                continue
                
            if v.r:
                R or e('\tRemove %d from read set of rid=%d', t.tid, rid)
                v.r.remove(t)
            else:
                q or e('\tUh-oh! rid=%d was neither read nor written by me (v.r=%s)', rid, v.r)
                assert not 'reachable'

            x = v.clobber
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

        t.dcommit = t.icommit = t.durable = get_next_stamp()
        del in_flight[t.tid]

        for x in commits:
            i_commit(x)
                                  
        if ww_blocks:
            if t.last_write is not None:
                t.clobbers[t.last_write] = None
                
            for rid in t.clobbers:
                c,w = clobbering.pop(rid, no_clobber)
                if c is t:
                    if w:
                        X = Q and w.tid not in tid_watch
                        X or e('\tUnblocking pid=%s', w.tid)
                        clobbering[rid] = (w,None)
                        w.blocked = False
                        yield from sys_unpark(w.tid)
                    else:
                        Q or e('\tReleasing clobber slot on rid=%d', rid)
                        
                else:
                    # happens if we aborted due to WaitDepth
                    Q or e('\tLeaving pid=%s blocked on rid=%s, WW pid=%s',
                           w and w.tid or None, rid, c.tid)
                    clobbering[rid] = (c,w)

            for rid,(c,w) in clobbering.items():
                try:
                    assert not c or not w or not c.dcommit
                except AssertionError:
                    errlog('pid=%d blocked on D-committed rid=%d pid=%d!', w.tid, rid, c.tid)
                    raise
                    
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='red')
        tracker.on_finish(pid, False)
        tracker.on_finalize(pid)


    def fini():
        if in_flight:
            fail_if_wedged()
            errlog('\nFound %d live transactions at exit (oldest from tick %.2f):',
                   len(in_flight), min(t.begin for t in in_flight.values())/float(ONE_TICK))
            if not q:
                for t in in_flight.values():
                    errlog('\tpid=%s deps={%s}', t.tid, ' '.join(map(str, (x.tid for x in t.war))))

        errlog('''
        Stats:

        Total transactions: %d (%d failures, %d serialization failures)
        Total accesses:     %d (%d isolation failures)''',
               stats.tx_count, stats.tx_aborts, stats.ser_failures,
               stats.acc_count, stats.iso_failures)
        print_failure_causes(stats)
        histo_print(resp_histo, 'transaction response times')


    return NamedTuple(nrec=nrec, tx_begin=tx_create,
                      tx_read=tx_read, tx_write=tx_write,
                      tx_commit=tx_commit, tx_abort=tx_abort,
                      fini=fini,
                      begin_tracking=tracker.begin_tracking,
                      end_tracking=tracker.end_tracking)
        
                


def test_ssi2_db():

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
        db = make_ssi2_db(stats=stats, tracker=tracker,nrec=10, ww_blocks=True, verbose=True)
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

    test_ssi2_db()
    #simulator(make_benchmark(make_db=make_si_db, nclients=50, max_inflight=100, nrec=1000, duration=10000), log=log_svg_task())
