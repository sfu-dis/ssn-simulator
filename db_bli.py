import collections, random, sys, traceback
from dbsim import *

class EagerFailWrite(AbortTransaction): pass
class EagerFailWriteDep(AbortTransaction): pass
class EagerFailReadDep(AbortTransaction): pass
class EagerFailReadAntiDep(AbortTransaction): pass
class EarlyPrecommitFail(AbortTransaction): pass
class PrecommitFail(AbortTransaction): pass

class ReadSkew(AbortTransaction):
    '''Read two different versions'''
    pass

class WriteSkew(AbortTransaction):
    '''Overwrote a version that depends on me'''
    
def make_db(tracker, stats, nrec, tid_watch, rid_watch, read_only_opt,
            verbose, **extra_kwargs):
    '''Blacklist isolation (BLI) offers a restricted form of Read
    Committed that is serializable. It works by tracking two kinds of
    commit: d-commit corresponds to the normal commit point, where a
    transaction has completed pre-commit successfully and can no
    longer be rolled back. At some point not preceding d-commit, a
    transaction will also i-commit. The latter is the point at which a
    transaction's predecessors have all i-committed, thus securing its
    place in the serial dependency graph (and confirming its
    cycle-free status, though SSN will prevent cycles in practice).

    Under BLI, transactions always read the most recent version
    possible, so long as the version has i-committed. However,
    i-commit can be arbitrarily far in the past, and so is even more
    prone to read/write divergence than SI. This is where the
    blacklist comes in: each transaction maintains a set of its direct
    predecessors while in flight, and at pre-commit it computes the
    set of all predecessors (direct or transitive) who have not yet
    i-committed. Any reader arriving later on can safely read the
    d-committed version as long as it does not appear in this
    transitive predecessor set.
    
    The key motivation behind blacklist isolation (BLI) is that we want
    to minimize the number of r:W anti-dependencies where the writer
    commits before the reader. The reason is two-fold. First, and most
    obvious, every dependency cycle must involve at least one such r:w
    edge, and so reducing their population reduces the chances of a
    cycle arising. Second, the SSN test always kills the reader in a
    problematic r:w edge, and under high contention one "bad" writer
    can kill many readers in this way. Once contention is sufficiently
    high, SSI actually outperforms SSN+SI because the former tends to
    kill the writer in a problematic r:w edge.

    '''
    q,e = not verbose, errlog

    tx_sstamps = {1:1}
    tx_pstamps = collections.defaultdict(lambda:0)
    tx_reads = collections.defaultdict(dict)
    tx_writes = collections.defaultdict(set)
    
    # tracks all transactions that have not yet i-committed, or which
    # i-committed since the last safe point. If a tid is not here, it
    # i-committed before the last safe point.
    tx_icommits = {}

    tx_deps,tx_depstar = {},{}
    tx_rwdeps = collections.defaultdict(set)
    tx_iblocks = collections.defaultdict(set)

    '''Each version V is associated with several pieces of information:

    - TID of its creator (the tracker manages this for us). Note that
      the TID may belong to an "old" transaction that the system has
      forgotten, in which case we won't be able to extract any further
      information about it.

    - Creation time. The tracker manages this also. Crucially, the
      timestamp remains in the version even after the transaction that
      creates it has been forgotten.

    - Read stamp. The most recent commit stamp of any transaction that
      read this version. Every reader must update this stamp on any
      version that has not been overwritten before the read
      commits. The read stamp is used to update p(T) of the
      transaction that eventually overwrites this version.
    
    - Active reader. Each version can store the TID of one transaction
      that is currently reading it. A transaction does not have to
      update the read stamp if it can leave its TID instead, reducing
      the amount of work it performs at pre-commit. Any reader or
      overwrite that comes later will update the read stamp with the
      active reader's commit stamp. If an active reader is forgotten
      before another transaction accesses the version, its commit time
      is not available; in these cases, the current safe point is used
      instead. This may cause some extra false positives in cycle
      detection, but not many (it would require a chain of
      anti-dependencies to span several epochs. If the loss of
      precision is unacceptable, the active reader slot should be
      eliminated, forcing all readers to set read stamps.
    
    - NOTE: We do *NOT* track s(T) of V's creator. T will not be
      forgotten until V dies, so any TR that is able to access V can
      still look up s(T).

    '''

    '''Life cycle of a version:

    Invisible: the creating transaction has not yet committed, nobody
    else can see this version. No locks are necessary, but the creator
    must record it in a write set.

    Visible: the creating transaction has committed and no delete has
    committed yet. Any read or delete that arrives before the next
    safe point must account for c(xmin) in its p(T). A reader that
    commits before any deletion cannot have a dangerous r:w conflict,
    but must still acquire an SIREAD lock. If the reader delays lock
    acquisigion until pre-commit, it can take over any existing SIREAD
    lock (which by definition must be older) instead of acquiring a
    new one. The lock can be released at the next safe point, and a
    writer that arrives before that will use the reader's c(T) to
    update its own p(T); a late-arriving writer will use the safe
    point to update its p(T) instead.

    Deleted: the version has been deleted, but some readers may still
    see it. The writer can release the SIREAD lock (if any), and no
    further SIREAD locks are necessary. The writer's identity is
    embedded in the version as xmax, and any reader that follows will
    use s(xmax) to update its own s(T).

    Dead: a new safe point has arrived since the version was deleted,
    and nobody can see the version any more. The system can safely
    forget c(xmax) and s(xmax).

    

    '''
    # we need to track all in-flight readers and the most
    # recently-committed reader of each unclobbered version. These are
    # the equivalent to P&G's SIREAD locks. Our version is less
    # memory-intensive, though, because each version retains at most
    # one committed lock---that of the most recent reader---and even
    # that one lock can be discarded at the next safe point.
    v_readers,v_rstamps = collections.defaultdict(set),{}

    def get_rstamp(rid, dep):
        return v_rstamps.get((rid,dep), 0)
        
    def tx_read(pid, rid, for_update=False):
        Q = q and pid not in tid_watch and rid not in rid_watch

        '''Postgresql does not track records as sequences of
        versions. Instead, each record is more or less independent and
        contains two transaction ids (xids): xmin and xmax, which are,
        respectively, the transaction that created and deleted the
        version in question.

        We achieve the same effect here (since our simulated
        microbenchmark does not have inserts or deletes) by treating
        the xmin of the clobbering version as xmax.

        '''
        xmin = tx_reads.get(pid, {}).get(rid, None)
        if xmin is not None:
            return xmin

        Q or e('read: pid=%s rid=%s', pid, rid)
        y,xprev,xmax = None,None,None
        pt,st = tx_pstamps[pid],tx_sstamps.get(pid, None)
        def read_filter(it):
            # take the newest version that we dare
            nonlocal xmax
            for x,xmin,_ in it:
                dstar = tx_depstar.get(xmin, ())
                Q or e('\tExamining depstar of version dep=%d: {%s}',
                       xmin, ' '.join(map(str,sorted(dstar))))
                if pid == xmin or pid not in dstar:
                    return x
                    
                xmax = xmin

            errlog('Oops! T=%d finds no suitable version of R=%d', pid, rid)
            assert not 'reachable'

        # do not rely on version commit stamps
        xmin,_ = yield from tracker.on_access(pid, rid, read_filter)

        # deal with inbound dependency edge
        if not tracker.is_known(xmin) or not tx_deps.get(xmin, None):
            cxmin = tracker.get_safe_point()
        else:
            cxmin = tracker.is_committed(xmin)
            assert cxmin
            
        Q or e('\tInbound dependency: T=%d p(T)=%d X=%d c(X)=%s',
               pid, pt, xmin, cxmin)
        if cxmin > pt:
            pt = tx_pstamps[pid] = cxmin
        if st and not (pt < st):
            raise EagerFailReadDep
               
        # check for a committed outbound anti-dependency edge. The
        # clobber (if any) cannot have yet been finalized, else we
        # would have used that version instead, so we can grab s(T) of
        # the overwriter (which must exist)
        if xmax:
            # reading under a committed overwrite
            '''WARNING:

            Because pgsql does not track records as sequences of
            versions, and because it allows deletions, it could
            encounter a finalized xmax. In that case the version is
            simply not visible, and so we cannot acquire a r:w
            conflict through it.

            In our case, though, we should have seen the newer version
            and it would be an error not to have chosen it.

            '''
            assert tracker.is_known(xmax)
            sxmax = tx_sstamps[xmax]
            Q or e('\tCommitted overwrite -> sxmax=%s', sxmax)
            assert sxmax
            assert xmax in tx_deps
            Q or e('\tOutbound anti-dependency: T=%d s(T)=%s X=%d s(X)=%s',
                   pid, st, xmax, sxmax)
            if not st or sxmax < st:
                st = tx_sstamps[pid] = sxmax
                
            if st and not (pt < st):
                raise EagerFailReadAntiDep
                
        elif xmin != pid:
            # reading somebody else's write, clobber yet to come
            x = tx_reads[pid].setdefault(rid, xmin)
            if x != xmin:
                assert x != pid
                # oops, already read a different version, and
                # neither was created by this transaction
                raise ReadSkew
            sxmax = None

        else:
            Q or e('\tNo committed overwrite found -> sxmax=None')
            sxmax = None

        v_readers[rid,xmin].add(pid)
            
        xmax,_ = tracker.get_overwriter(rid, xmin, True)
        if xmax:
            # reading under an overwrite
            Q or e('\tpid=%d records outbound r:w dep to X=%d', pid, xmax)
            tx_rwdeps[xmax].add(pid)

        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (xmin, rid))
        return xmin
        

    def tx_write(pid, rid):
        Q = q and pid not in tid_watch and rid not in rid_watch
        Q or e('write: pid=%s rid=%d', pid, rid)

        # do the write (don't depend on the version stamp)
        dep,_ = yield from tracker.on_access(pid, rid, False)
        
        # have I written this before?
        if dep == pid:
            q or e('\talready wrote to this record')
            return

        # Did we previously read a different version?
        rdep = tx_reads.get(pid, {}).get(rid, dep)
        if rdep != dep:
            raise ReadSkew

        # can I actually see the version I'm overwriting?
        if not tracker.is_known(dep):
            cdep = tracker.get_safe_point()
        elif pid in tx_depstar.get(dep,()):
            raise WriteSkew
        else:
            cdep = tracker.get_end(dep)
            
        # deal with inbound dependency edge
        pt,st = tx_pstamps[pid],tx_sstamps.get(pid,None)
        Q or e('\tInbound dependency: T=%d p(T)=%d X=%d c(X)=%d',
               pid, pt, dep, cdep)
        if pt < cdep:
            pt = tx_pstamps[pid] = cdep
        if st and not (pt < st):
            raise EagerFailWriteDep

        # check for inbound anti-dependency edges
        rstamp = get_rstamp(rid,dep)
        Q or e('\tDeal with committed inbound anti-deps: T=%d p(T)=%d r(V)=%d',
               pid, pt, rstamp)
        if pt < rstamp:
            pt = tx_pstamps[pid] = rstamp
            if st and not (pt < st):
                q or e('EagerFailWrite pid=%d', pid)
                raise EagerFailWrite

        # record the write
        tx_writes[pid].add((rid,dep))
        readers,rwdeps = v_readers[rid,dep],tx_rwdeps[pid]
        readers.discard(pid)
        for x in v_readers[rid,dep]:
            rwdeps.add(x)
        
        # no outbound edges possible
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='%s=db[%s]' % (dep, rid))
        
    tid_watched = set()
    def tx_create(pid, is_readonly):
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
        tx_deps[pid] = set([pid])
        if pid == 591:
            errlog('pid=%d deps=%s id=%s', pid, tx_deps[pid], id(tx_deps))

    def tx_commit(pid):
        Q = q and pid not in tid_watch
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        Q or e('Commit %s', pid)
        if pid in tid_watch:
            tid_watched.add(pid)

        # get a serialization point, not reused even if we abort
        end = tracker.on_precommit(pid)
        if read_only_opt and pid not in tx_writes:
            ct = tracker.get_begin(pid)
        pt,st = tx_pstamps[pid],tx_sstamps.setdefault(pid, end)
        if st == end:
            Q or e('\tT=%d initialized s(T)=%s at pre-commit', pid, end)
        else:
            Q or e('\tT=%d has s(T)=%s at start of pre-commit', pid, end)
        
        # /// BEGIN CRITICAL SECTION ///

        # read-only optimization: I have to update rstamps of all
        # remembered reads, but if I finished read-only then I can use
        # my begin timestamp instead of my end timestamp, thus
        # reducing the probability that I violate some overwriter's
        # exclusion window. In case of forgotten reads, they will get
        # the right value by calling get_version_reader().
        ct = end
        if read_only_opt and pid not in tx_writes:
            ct = tracker.get_begin(pid)

        # check for reads that commit under an overwrite
        safe_point = tracker.get_safe_point()
        for rid,dep in tx_reads.get(pid, {}).items():
            rstamp = get_rstamp(rid,dep)
            Q or e('\tT=%d verifying read of R%d/T%d', pid, rid, dep)

            # skip versions we overwrote
            if (rid,dep) in tx_writes.get(pid, ()):
                continue
                
            # Check for overwrites. If no clobber has shown up yet,
            # force an rstamp update.
            x,_ = tracker.get_overwriter(rid, dep)
            if not x:
                # no committed overwrite, have to update rstamp
                Q or e('\tUpdate r(V): T=%d c(T)=%d r(V)=%d',
                       pid, ct, rstamp)
                if rstamp < ct:
                    rstamp = v_rstamps[rid,dep] = ct

            else:
                # overwrite committed after we read, so update s(T)
                if not tracker.is_known(x):
                    sx = -1
                else:
                    sx = tx_sstamps.get(x, None)
                Q or e('\tCommitted clobber: T=%d s(T)=%d X=%d s(X)=%s',
                       pid, st, x, sx)
                if sx and st > sx:
                    st = tx_sstamps[pid] = sx

        for rid,dep in tx_writes.get(pid,()):
            Q or e('\tT=%d verifying overwrite of R%d/T%d', pid, rid, dep)
            rstamp = get_rstamp(rid,dep)
            Q or e('\tAccount for committed readers: T=%d p(T)=%d r(V)=%d',
                   pid, pt, rstamp)
            if pt < rstamp:
                pt = tx_pstamps[pid] = rstamp

        if not (pt < st):
            Q or e('\tPrecommitFail pid=%d p(T)=%d s(T)=%d', pid, pt, st)
            raise PrecommitFail

        dstar = tx_depstar[pid] = set()
        deps = tx_deps[pid]
        if pid == 591:
            errlog('pid=%d deps=%s id=%s', pid, tx_deps[pid], id(tx_deps))
        deps.remove(pid)
        
        for rid,dep in tx_writes.get(pid, ()):
            # update deps and depstar
            xdeps = tx_deps.get(dep, None)
            if xdeps and dep not in deps:
                deps.add(dep)
                tx_iblocks[dep].add(pid)
                dstar.update(d for d in tx_depstar[dep] if d in tx_deps)

            # account for readers we clobber
            for x in v_readers[rid,dep]:
                tx_iblocks[x].add(pid)
                deps.add(x)
                dstar.add(x)

        for rid,dep in tx_reads.get(pid, {}).items():
            v_readers[rid,dep].discard(pid)
            
            # skip versions we overwrote
            if (rid,dep) in tx_writes.get(pid, ()):
                continue
                
            # update deps and depstar
            xdeps = tx_deps.get(dep, None)
            if xdeps and dep not in deps:
                deps.add(dep)
                tx_iblocks[dep].add(pid)
                dstar.update(d for d in tx_depstar[dep] if d in tx_deps)
                
            # Check for overwrites. If no clobber has shown up yet,
            # force an rstamp update.
            x,_ = tracker.get_overwriter(rid, dep)
            if x:
                # committed overwrite, update its dep/depstar
                xdeps = tx_deps.get(x, None)
                if xdeps:
                    # not already i-committed
                    xdeps.add(pid)
                    tx_depstar[x].add(pid)
                    
        # add incoming r:w edges as well, but not transitively
        for x in tx_rwdeps.get(pid,()):
            tx_iblocks[x].add(pid)
            deps.add(x)
            dstar.add(x)

        
        Q or e('\tT=%d at commit: p(T)=%d s(T)=%d', pid, pt, st)
        Q or e('\tT=%d deps={%s} and depstar={%s}', pid,
               ' '.join(map(str,sorted(deps))),
               ' '.join(map(str,sorted(dstar))))
        # /// END CRITICAL SECTION ///

        tx_reads.pop(pid,None)
        tx_pstamps.pop(pid,None)
        if not deps and not tx_rwdeps.get(pid, None):
            i_commit(pid)
            
        yield from tracker.on_finish(pid, True, finish_callback)
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        
    def tx_abort(pid):
        Q = q and pid not in tid_watch
        if pid in tid_watch:
            tid_watched.add(pid)

        tx_deps.pop(pid, None)
        tx_rwdeps.pop(pid, None)
        i_commit(pid)
        tx_reads.pop(pid,None)
        tx_pstamps.pop(pid,None)
        Q or e('Abort %d', pid)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='red')
        yield from tracker.on_finish(pid, False, finish_callback)

    def try_icommit(pending, pid, x):
        Q = q and pid not in tid_watch
        xdeps = tx_deps.get(x, ())
        if xdeps:
            xdeps.discard(pid)
            finished = not tracker.is_known(x) or tracker.is_committed(x)
            rwdeps = tx_rwdeps.get(x, None)
            if finished and not xdeps and not rwdeps:
                pending.append(x)
            else:
                Q or e('\ttid=%d did not i-commit: finished=%s deps={%s} rwdeps={%s}', x, finished, ' '.join(map(str,sorted(xdeps))), ' '.join(map(str,sorted(rwdeps))))
        
    def i_commit(pid):
        assert not tx_deps.get(pid, None)
        assert not tx_rwdeps.get(pid, None)
        assert tracker.is_known(pid)
        
        pending = collections.deque()
        pending.append(pid)
        while pending:
            pid = pending.popleft()
            Q = q and pid not in tid_watch
            Q or e('I-commit pid=%d', pid)
            for rid,dep in tx_writes.get(pid, ()):
                x = tracker.get_overwriter(rid, dep)
                try_icommit(pending, pid, x)

                for x in v_readers[rid, pid]:
                    try_icommit(pending, pid, x)
                    

            for rid,dep in tx_reads.get(pid, {}).items():
                v_readers[rid,dep].discard(pid)
                x = tracker.get_overwriter(rid, dep)
                try_icommit(pending, pid, x)

        #errlog('iblocks=%s', tx_iblocks.get(pid, None))
        #for x in tx_iblocks.pop(pid, ()):
        #    try_icommit(x, tx_deps[x], tx_rwdeps.get(x, ()))
            
        tx_deps.pop(pid, None)
        tx_rwdeps.pop(pid, None)
        tx_depstar.pop(pid, None)

    def finish_callback(pid):
        Q = q and pid not in tid_watch
        Q or e('Forget pid=%d', pid)
        tx_sstamps.pop(pid,None)
        tx_writes.pop(pid,None)
        
    def fini():
        print_general_stats(stats)
        print_failure_causes(stats)


    return NamedTuple(nrec=nrec, tx_begin=tx_create,
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
        db = make_db(stats=stats, tracker=tracker,nrec=10, ww_blocks=True, verbose=True)
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
    #simulator(make_benchmark(make_db=make_si_db, nclients=50, max_inflight=100, nrec=1000, duration=10000), log=log_svg_task())
