import collections, random, sys
from dbsim import *

0 and errlog('''FIXME:
python3 main.py --db-model=2pl --nclients=112 --seed=t70aQONy --duration=10000

Somehow suffers an undetected deadlock.
''')

class DeadlockDetected(AbortTransaction):
    '''Transaction must abort due to deadlock'''
    pass

class UpgradeFailed(AbortTransaction):
    '''Upgrade failure'''
    pass

class WaitLimited(AbortTransaction):
    '''Transaction would exceed maximum wait depth'''
    pass

def make_db(tracker, stats, db_size, verbose, limit_wait_depth, **extra_kwargs):
    '''A database model implementing strict two phase locking
    (2PL). Transactions must acquire appropriate locks before
    accessing data, and may block (or even deadlock) when requesting
    locks already held by other transactions. Neither isolation nor
    serialization failures are possible.
    '''
    stats.lock_waits,stats.deadlocks = 0,0
    
    wait_histo = collections.defaultdict(lambda:0)
    resp_histo = collections.defaultdict(lambda:0)
    dlock_histo = collections.defaultdict(lambda:0)
    in_flight = {}
    tx_logs = collections.defaultdict(dict)
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

    '''We support the following lock modes:

    N - no lock (compat: R, U, X)
    R - read lock (compat: R, U; non-blocking upgrade)
    S - multiple read locks held (compat: R, U)
    U - read lock with intent to upgrade (compat: R; non-blocking upgrade)
    V - one U and at least one R (compat: R)
    X - exclusive write lock (compat: None)
    P - upgrade to X in progress (compat: None)
    W - lock blocked (compat: None)

    States N, S, V, P and W are synthetic lock modes used internally
    by the lock manager. A lock may be in such a mode, but no
    transaction requests it specifically. S (V) is identical to R (U),
    except that the former signals the lock manager that a lock holder
    will block on attempting to upgrade, while the latter allows the
    (single) holder to upgrade to X without blocking. P indicates that
    some request is blocked on upgrade (see below). These extra modes
    allow upgraders to make decisions based only on the lock's current
    mode, without examining the request list.

    To keep lock tables small, we assign the smallest ordinals to lock
    modes that transactions are allowed to request.

    '''
    R, U, X, P, N, S, V, W = 0, 1, 2, 3, 4, 5, 6, 7
    mode_names = dict(N=N, R=R, U=U, X=X, P=P, S=S, V=V, W=W)
    mode_names = dict((v,k) for (k,v) in mode_names.items())
    mode_name = lambda m: mode_names.get(m, m)

    '''The supremum table is used to determine compatibility efficiently,
    and new lock requests rely heavily on it. The value returned by
    supr[current_mode][requested_mode] indicates the effective lock
    mode caused by the arrival of the new mode. W indicates an
    incompatible request that must block.

    '''
    _,supr = W,[None]*8
    
    #           R  U  X  P  N   
    supr[N] = ( R, U, X, P, N )
    supr[R] = ( S, V, _, _, R )
    supr[S] = ( S, V, _, _, S )
    supr[U] = ( V, _, _, _, U )
    supr[V] = ( V, _, _, _, V )
    supr[P] = ( _, _, _, _, P )
    supr[X] = ( _, _, _, _, X )
    supr[_] = ( _, _, _, _, _ )

    '''Lock upgrades are a pain, because they put the transaction in a
    position of both holding and needing to acquire a lock... we also
    need to be able to process every possible type of upgrade, because
    users could accidentally (or maliciously) do things like read,
    read-for-update, then write (for example).

    Many upgrade requests either succeed or deadlock immediately;
    those that remain (R->X and U->X, with readers present) must
    block. We implement the blocking as follows. First, the request
    moves from the holders list to the upgraders list and its mode
    changes from U to P (lock mode changes to P as well). P is an
    asymmetric mode, where supr[*][P] = supr[*][U] and supr[P][*] =
    supr[X][*]. So, while R-U-R is possible, R-P-R is not (ensuring
    the upgrade request does not starve); similarly, R-X is not
    possible, but R-P is (allowing the upgrade to occur). Once the
    upgrader unblocks, it can safely change its mode (and that of the
    lock) from P to X without affecting its successors.

    The logic that decides whether to succeed/block/deadlock is
    encoded into a three-dimensional lookup:

    upgr[lock.m, req.m, mode] = (rmode, gmode, smode)

    Where rmode is the mode to request, gmode is the mode to change to
    after the request is granted, and smode is the new lock supremum
    to use once the request is granted. An empty entry means deadlock,
    while a missing one means an illegal upgrade has been requested
    (e.g. upgr[X][R][U]). If rmode != gmode, the upgrade request must
    block (going to the "upgrader" position) and the lock goes into
    rmode for the interim. With six lock modes, three of which that
    can be requested and upgraded to by transactions, we have 6*3*3 =
    54 possible situations:

    '''
    upgr = {}
    upgr[R, R, R] = (R, R, R)	# trivial
    upgr[R, R, U] = (U, U, U)
    upgr[R, R, X] = (X, X, X)
    #upgr[R, U, *] =              illegal
    #upgr[R, X, *] =              illegal

    upgr[S, R, R] = (S, R, R)	# trivial	
    upgr[S, R, U] = (V, U, U)
    upgr[S, R, X] = (X, P, X)
    #upgr[S, U, *] =              illegal
    #upgr[S, X, *] =              illegal
    
    upgr[U, R, R] = (U, R, R)	# trivial
    upgr[U, R, U] = None
    upgr[U, R, X] = None
    upgr[U, U, R] = (U, U, U)	# trivial
    upgr[U, U, U] = (U, U, U)	# trivial
    upgr[U, U, X] = (X, X, X)
    #upgr[U, X, *] =              illegal
    
    upgr[V, R, R] = (V, R, R)	# trivial
    upgr[V, R, U] = None
    upgr[V, R, X] = None
    upgr[V, U, R] = (V, U, U)	# trivial
    upgr[V, U, U] = (V, U, U)	# trivial
    upgr[V, U, X] = (X, P, X)
    #upgr[V, X, *] =              illegal
    
    upgr[P, R, R] = (P, R, R)	# trivial
    upgr[P, R, U] = None
    upgr[P, R, X] = None
    #upgr[P, U, *] =              illegal
    #upgr[P, X, *] =              illegal

    #upgr[X, R, *] =              illegal    
    #upgr[X, U, *] =              illegal
    upgr[X, X, R] = (X, X, X)	# trivial
    upgr[X, X, U] = (X, X, X)	# trivial
    upgr[X, X, X] = (X, X, X)	# trivial
    '''
    30 of the cases are illegal, 12 are trivial (lock strength
    unchanged), six deadlock, four succeed immediately, and two block.

    If rmode != gmode, then the request must block and the lock state
    is set to rmode for the interim. Otherwise, the request is either
    trivial or succeeds immediately (we don't actually care which).
    
    NOTE: If we add intent lock modes to the mix, then many more kinds of
    upgrade become possible (e.g. IR -> {R, IX} -> RIX -> UIX -> X)
    but we should be able to handle the blocking cases in the same
    way, by introducing more lock modes. The main issue is that intent
    modes allow multiple upgrades to proceed (and succeed) at the same
    time (e.g. IR IR IX -> R R), which might complicate setting of the
    final lock mode (IR -> R implies R, but IR IR -> R R implies
    S). We expect that asymmetric transient modes should cover this,
    but that has not been pondered heavily, let alone proven.
    '''
    class LockRequest(object):
        def __init__(self, pid, mode):
            self.pid, self.m = pid,mode
        def __repr__(self):
            return 'LockRequest(%s, %s)' % (self.pid, mode_name(self.m))
        def __str__(self):
            return '%s:%s' % (self.pid, mode_name(self.m))

    '''We partition a lock's request list into three segments: holders,
    upgraders (those who already hold the lock but who wait to acquire
    a stronger mode), and waiters (those who wish to acquire the lock
    but are currently blocked from doing so). The lock also maintains
    two modes: its "wait" mode (for new requests), and its "held" mode
    (for upgrades).

    '''
    class Lock(object):
        def __init__(self):
            self.m, self.wm, self.holders, self.upgrader = N,N,[],None
            self.waitlist = collections.deque()
        def __repr__(self):
            return ('Lock(m=%s/%s, h=[%s], u=%s, w=[%s])'
                    % (mode_name(self.m), mode_name(self.wm),
                       ' '.join(map(str, self.holders)),
                    str(self.upgrader), ' '.join(map(str, self.waitlist))))

    locks = collections.defaultdict(Lock)
    tx_locks = collections.defaultdict(dict)
    tx_deps, tx_block = {}, {}
    empty_set, empty_dict = set(), {}
    
    def lock_acquire(pid, rid, mode):
        now = yield from sys_now()
        must_wait,is_upgrade = False,False
        lock = locks[rid]
        orig_m, orig_wm = lock.m, lock.wm
        
        q or e('lock_acquire: t=%.2f pid=%d rid=%d lock=%s/%s mode=%s',
               now/float(ONE_TICK), pid, rid,
               mode_name(lock.m), mode_name(lock.wm), mode_name(mode))
        my_locks = tx_locks[pid]
        req = my_locks.get(rid, None)
        
        # /// BEGIN CRITICAL SECTION ///
        '''WARNING: Bad Things Happen (tm) if we allow any scheduling to occur
        after starting to acquire a lock but before checking for (and
        responding to) a possible deadlock. Other threads could join
        this thread's pending deadlock in ways that aborting this
        transaction won't fix.

        '''
        if req:
            # existing request, may need upgrade
            try:
                smode,rmode,gmode = upgr[lock.m, req.m, mode]
            except TypeError:
                q or e('\tUpgrade deadlock detected: lock.m=%s/%s req.m=%s/%s',
                       mode_name(lock.m), mode_name(lock.wm),
                       mode_name(req.m), mode_name(mode))
                histo_add(dlock_histo, 0)
                raise UpgradeFailed from None
            except KeyError as err:
                err.args = map(mode_name, err.args[0])
                raise

            if rmode == gmode:
                q or e('\tUpgrade from %s to %s (%s) succeeds immediately',
                       mode_name(req.m), mode_name(mode), mode_name(gmode))
                if lock.m == lock.wm:
                    lock.wm = smode
                lock.m,req.m = smode,gmode
                return

            if lock.upgrader:
                # somebody else is already blocked on upgrade, no go.
                q or e('\tcollided with another upgrader')
                histo_add(dlock_histo, 0)
                raise UpgradeFailed
                
            try:
                assert not lock.upgrader
            except:
                errlog('upgrader found during %s->%s upgrade: pid=%s rid=%s lock.m=%s/%s holders=%s upgrader=%s',
                       mode_name(req.m), mode_name(mode), pid, rid, mode_name(lock.m),
                       mode_name(lock.wm), lock.holders, lock.upgrader)
                raise
                
            # abandon current request and block with a new one
            orig_reqm = req.m
            req.m = N
            req = my_locks[rid] = LockRequest(pid, rmode)

            # mark the lock as upgrading
            if lock.wm == lock.m:
                lock.wm = rmode
            lock.m = rmode
            lock.upgrader = req

            try:
                check_deadlock(lock, req, set())
            except AbortTransaction:
                # revert to the original (weaker but granted) request
                lock.m,lock.wm,req.m = orig_m,orig_wm,orig_reqm
                lock.holders.append(req)
                lock.upgrader = None
                # /// END CRITICAL SECTION ///
                yield from sys_busy(ONE_TICK, color='red', title='deadlock!')
                raise

            # /// END CRITICAL SECTION ///
            yield from sys_busy(ONE_TICK//10, color='pink',
                                title='Upgrade: rid=%s lock.m=%s req.m=%s rmode=%s'
                                % (rid, lock.m, req.m, rmode))
            yield from lock_block(lock, pid)
            
            # patch things up when we come back
            q or e('after %s->%s upgrade: rid=%s lock.m=%s holders=%s',
                   mode_name(orig_reqm), mode_name(gmode), rid, mode_name(lock.m), lock.holders)
            lock.m,req.m = smode,gmode
        else:
            req = my_locks[rid] = LockRequest(pid, mode)
            lock.wm = supr[lock.wm][req.m]
            if lock.wm != W:
                q or e('\tRequest granted: lock.m=%s', mode_name(lock.wm))
                assert not lock.upgrader and not lock.waitlist
                lock.holders.append(req)
                lock.m = lock.wm
                return

            '''Prep for DLD before inserting the request.

            lock_block() performs deadlock detection and deals with
            dependencies on lock holders automatically, but we are
            responsible to calculate dependencies arising from the
            current upgrader (if any) and all incompatible waiters.

            NOTE: there is some set of immediate predecessors
            (possibly empty) which are compatible with each other and
            with the incoming request. We avoid false dependencies by
            processing the waitlist backwards and skipping
            predecessors until the computed supremum goes to W.

            '''
            my_deps = set()
            if lock.upgrader:
                my_deps.add(lock.upgrader.pid)

            smode = req.m
            for dep in reversed(lock.waitlist):
                smode = supr[smode][dep.m]
                if smode == W:
                    my_deps.add(dep.pid)
                    
            # enqueue the request and block
            lock.waitlist.append(req)

            try:
                check_deadlock(lock, req, my_deps)
            except DeadlockDetected:
                # remove all traces of the new (never granted) request
                assert req is lock.waitlist[-1]
                lock.waitlist.pop()
                lock.m,lock.wm = orig_m,orig_wm
                del my_locks[rid]
                # /// END CRITICAL SECTION ///
                yield from sys_busy(ONE_TICK, color='red', title='deadlock!')
                raise

            # /// END CRITICAL SECTION ///
            yield from sys_busy(ONE_TICK, color='fuchsia',
                                title='Request: rid=%s lock.m=%s/%s mode=%s'
                                % (rid, mode_name(lock.m), mode_name(lock.wm), mode_name(req.m)))
            yield from lock_block(lock, pid)

    def lock_block(lock, pid):
        # no deadlock, proceed to block
        if tracker.is_tracked(pid):
            stats.lock_waits += 1
            
        q or e('\tblocked (lock is %s/%s, deps: %s)',
               mode_name(lock.m), mode_name(lock.wm),
               ' '.join(map(str, sorted(my_deps))))
        delay = yield from sys_park()
        histo_add(wait_histo, delay)
        q or e('\tpid %d unblocked at t=%.2f (lock is %s)',
               pid, (yield from sys_now())/float(ONE_TICK), mode_name(lock.m))
        del tx_deps[pid]
        del tx_block[pid]
            
        

    def print_deadlock(scc):
        errlog('digraph deadlock {')
        errlog('\tconcentrate="true"')
        for dep in scc:
            errlog('\t"tx_%d" [ shape="ellipse" ]', dep)
        rids = set(rid for dep in scc
                   for rid,req in tx_locks.get(dep, empty_dict).items())
        for rid in rids:
            lock = locks[rid]
            errlog('\t"rid_%d" [ shape="box" label="%d: %s/%s" ]',
                   rid, rid, mode_name(lock.m), mode_name(lock.wm))
            for req in lock.holders:
                if req.m != N:
                    errlog('\t"rid_%d" -> "tx_%d" [ label="%s" ]',
                           rid, req.pid, mode_name(req.m))
            if lock.upgrader:
                errlog('\t"rid_%d" -> "tx_%d" [ label="%s" ]',
                       rid, req.pid, mode_name(req.m))
                errlog('\t"tx_%d" -> "rid_%d"', req.pid, rid)
            for i,req in enumerate(lock.waitlist):
                if req.m != N:
                    errlog('\t"tx_%d" -> "rid_%d" [ label="%s [%d]" ]',
                           req.pid, rid, mode_name(req.m), i)

        errlog('}\n')
        
    def check_deadlock(lock, req, my_deps):
        '''Dreadlocks-based deadlock detection

        When any request blocks, it acquires direct dependencies on
        all holders, and inherits indirectly any dependencies acquired
        by holders that are currently blocked on other locks.

        When a new request blocks, it acquires additional dependencies
        on the upgrade request and any incompatible waiting
        requests. Our caller is responsible for both.

        A couple of pitfalls: when queueing up behind other members of
        the waitlist, it's tempting to just "borrow" the dependencies
        they already calculated. However, but we could miss a deadlock
        if a transaction in H blocked after the other transaction
        generated its dependency set. It's also tempting to look at
        the dependency lists and declare a false positive if some of
        them are awake. Unfortunately, it is possible for a
        transaction to form 2+ deadlock cycles with a single request,
        and we have no way to know whether the wakeful dependencies
        break all deadlocks or only some of them.

        '''
        is_upgrade = not my_deps
        my_deps.update(dep.pid for dep in lock.holders if dep.m != N)
        pid = req.pid
        tx_block[pid],tx_deps[pid] = lock,my_deps
        
        if limit_wait_depth:
            blocked = list(d.pid
                           for d in lock.holders
                           if d.m != N and d.pid in tx_block)
            if limit_wait_depth > 1:
                blocked = list(d.pid
                               for b in blocked for d in tx_block[b].holders
                               if d.m != N and d.pid in tx_block)

            if blocked:
                del tx_deps[pid]
                del tx_block[pid]
                raise WaitLimited
            
        scc = tarjan_incycle(tx_deps, pid)
        #q = False
        if scc:
            q or e('\tDeadlock detected%s: pid=%s %s',
                   ' (upgrade)' if is_upgrade else '', pid,
                   ' '.join(map(str, sorted(scc))))
            stats.deadlocks += 1
            if 0 and len(scc) == 10:
                print_deadlock(scc)
                        
            histo_add(dlock_histo, len(scc))
            del tx_deps[pid]
            del tx_block[pid]
            raise DeadlockDetected


    def lock_release(rid, req):
        lock = locks[rid]
        now = yield from sys_now()
        q or e('lock_release: t=%.2f pid=%d rid=%d mode=%s/%s',
               now/float(ONE_TICK), req.pid, rid,
               mode_name(lock.m), mode_name(lock.wm))
        
        lock.m,lock.wm,req.m = N,N,N
        i,h = 0, lock.holders
        while i < len(h):
            req = h[i]
            if req.m == N:
                # discard
                h[i] = h[-1]
                h.pop()
            else:
                # TODO: break if lock.m regains its original value?
                smode = supr[lock.m][req.m]
                if smode == W:
                    errlog('bad lock release: pid=%d rid=%d lock.m=%s req.m=%s holders=%s',
                           req.pid, rid, mode_name(lock.m), mode_name(req.m), lock.holders)
                assert smode != W
                lock.wm = lock.m = smode
                i += 1
                
        unblock, req = [], lock.upgrader
        if req:
            lock.wm = supr[lock.wm][req.m]
            q or e('\tprocess upgrade request: rid=%s pid=%s lock.m=%s/%s req.m=%s',
                   rid, req.pid, mode_name(lock.m), mode_name(lock.wm), mode_name(req.m))
            if lock.wm == W:
                return
                
            lock.m = lock.wm
            lock.upgrader = None
            h.append(req)
            unblock.append(req)
                
        w = lock.waitlist
        while len(w):
            req = w[0]
            q or e('\tprocess waiting request (pid=%s lock.m=%s/%s req.m=%s)',
                   req.pid, mode_name(lock.m), mode_name(lock.wm), mode_name(req.m))
            if req.m != N:
                lock.wm = supr[lock.wm][req.m]
                if lock.wm == W:
                    break

                lock.m = lock.wm
                h.append(req)
                unblock.append(req)
                
            w.popleft()

        if not h:
            assert not lock.upgrader
            assert not w
            del locks[rid]
            
        for req in unblock:
            assert req.m != N
            q or e('\tUnblocking %s', req)
            yield from sys_unpark(req.pid)

    def release_all_locks(pid):
        l = tx_locks.pop(pid, empty_dict)
        for rid,req in l.items():
            yield from lock_release(rid, req)
        
        
    def db_access(pid, rid, mode):
        yield from lock_acquire(pid, rid, mode)
        val = yield from tracker.on_access(pid, rid, mode != X)
        return val
            
    def tx_read(pid, rid, for_update=False):
        val = yield from db_access(pid, rid, U if for_update else R)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (val, rid))
        return val
        
    def tx_write(pid, rid):
        val = yield from db_access(pid, rid, X)
        tx_logs[pid].setdefault(rid, val)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='db[%s]=%s' % (rid, pid))

    def tx_create(pid, is_readonly=False):
        in_flight[pid] = yield from sys_now()
        tracker.on_begin(pid)
        
    def finish(pid):
        yield from release_all_locks(pid)
        then,now = in_flight.pop(pid), (yield from sys_now())
        histo_add(resp_histo, then-now)

    def tx_commit(pid):
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        tx_logs.pop(pid, None)
        yield from finish(pid)
        yield from tracker.on_finish(pid, True)
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        
    def tx_abort(pid):
        # log rollback is not free...
        log = tx_logs.pop(pid, empty_dict)
        for rid,val in log.items():
            yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                                color='lightsteelblue')
            
        yield from finish(pid)
        yield from tracker.on_finish(pid, False)

    def global_deadlock_check():
        # build a full dependency graph and check it for deadlocks
        deps = collections.defaultdict(set)
        for rid,lock in locks.items():
            # a lock depends on all current holders
            deps[-rid] = set(req.pid for req in lock.holders if req.m != N)
            # all upgrading/waiting pids depend on the lock
            if lock.upgrader:
                deps[lock.upgrader.pid].add(-rid)
            for req in lock.waitlist:
                if req.m != N:
                    deps[req.pid].add(-rid)

        deadlocks = tarjan_iter(deps)
        if deadlocks:
            errlog('\nDeadlocks found:')
            for sc in deadlocks:
                errlog('\n\t%s', ' '.join(map(str, sorted(sc))))
                print_deadlock(sc)
            return True
        else:
            errlog('\nNo deadlocks found')
            return False

    def end_tracking(callback):
        assert not global_deadlock_check()
        tracker.end_tracking(callback)
        
    def fini():
        if in_flight:
            errlog('\nFound %d live transactions at exit (oldest from tick %.2f)',
                   len(in_flight), min(in_flight.values())/float(ONE_TICK))

        global_deadlock_check()

        #try:
        #    assert not locks
        #    assert not tx_locks
        #    assert not tx_logs
        #except:
        #    errlog('\nLock table:\n\t%s',
        #           '\n\t'.join('rid=%s: %s' % x
        #                       for x in locks.items()))
        #    errlog('Transaction locks:\n\t%s',
        #           '\n\t'.join('pid=%s: %s' % (pid,' '.join('%s:%s' % (rid, mode_name(req.m)) for rid,req in rlist.items())) for pid,rlist in tx_locks.items()))
        #    errlog('Transaction logs: %s', tx_logs)
        #    #raise
            
        print_general_stats(stats)
        errlog('    Total lock waits:   %6d (%d deadlocks)',
               stats.lock_waits, stats.deadlocks)
        print_failure_causes(stats)
        histo_print(wait_histo, 'lock wait times')
        histo_print(resp_histo, 'transaction response times')
        histo_print(dlock_histo, 'deadlock sizes (0 = upgrade)', xtime=False)

    return NamedTuple(db_size=db_size, tx_begin=tx_create,
                      tx_read=tx_read, tx_write=tx_write,
                      tx_commit=tx_commit, tx_abort=tx_abort,
                      fini=fini,
                      begin_tracking=tracker.begin_tracking,
                      end_tracking=end_tracking)


def test_2pl_db():

    R,U,X = 1,2,3
    def test_fini(db):
        yield from sys_sleep(1000*ONE_TICK)
        db.fini()
        yield from sys_exit()

    def access(db, pid, rid, mode, delay):
        yield from sys_sleep(delay*ONE_TICK)
        yield from db.tx_write(pid, rid) if mode == X else db.tx_read(pid, rid, mode == U)

    def commit(db, pid, delay):
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

    for test in (test1,test2,test3,test4,test5,test6,test7):
        errlog('\n%s\nRunning test:\n\n\t%s\n', '='*72, test.__doc__)
        stats = NamedTuple()
        tracker = dependency_tracker(stats)
        db = make_2pl_db(stats=stats, tracker=tracker,db_size=10, verbose=True)
        simulator(test(db), log=log_svg_task())
        
        
    
if __name__ == '__main__':
    if 0:
        test_tarjan()
        test_2pl_db()
        exit(0)

    try:
        seed = sys.argv[1]
    except:
        seed = make_seed()

    errlog('Using RNG seed: %s', seed)
    random.seed(seed)

    simulator(make_benchmark(make_db=make_2pl_db, nclients=50, max_inflight=100, db_size=1000, duration=10000), log=log_svg_task())
