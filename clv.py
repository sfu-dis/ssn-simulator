import collections, getopt, heapq, itertools, random, sys

# how many records in the database?
NRECS = 100

# Simulate an open system rather than a closed one. Doing so adds
# realism and makes it harder to hide poor response times.  clients
# don't wait for a response before sending a new request, but rather
# track all transactions they have submitted which are still in
# flight. If the in-flight list for a client grows too long, it will
# stall with a warning.

NCLIENTS = 10

# we model a transaction consisting of 1+ reads followed by 0+
# writes. A read-modify-write is the same as a write from a
# concurrency control perspective, but we allow the possibility for
# later (seemingly independent) accesses to previously-read
# values. This maximizes the potential for Bad Things to occur
# (because the reads are vulnerable to updates while we wait for the
# writes to complete). There is little point to appending reads after
# the last write, because those can be served from a snapshot after
# releasing all locks. We expose three knobs: total number of
# accesses, percentage of those accesses which are to be writes (on
# average; R/W ratios for individual transactions vary), and the
# average length of the delay (if any) between the read and write phase of the
# transaction, measured in arbitrary time units we'll call
# "ticks". All data accesses are assumed to take one between one and
# two ticks, in the absence of lock conflicts. 
NACCESS = 10
PWRITE = 10
RWDELAY = 10
LOGDELAY = 100 

# TODO: Another tuning knob: how likely is a transaction to access a value a
# second time? This percentage is in addition to of any random
# re-access that might be chosen accidentally. 
PAGAIN = 0

# TODO; to avoid floating point weirdness, we should use a fixed-point
# representation for all timestamps, accurate to one part in 64k.

# inspired by http://www.dabeaz.com/coroutines/Coroutines.pdf
todos = []
now = [0]
db = dict()

ONE_TICK = 1000

def errlog(msg, *args):
    if args:
        msg %= args
    sys.stderr.write('%s\n' % msg)
    
def log_devnull():
    while 1:
        yield

# SVGPan is BSD-licensed code downloaded from
# http://www.cyberz.org/projects/SVGPan/SVGPan.js
class svg_printer(object):
    def __enter__(self):
        head = '''<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" version="1.1">
  <script xlink:href="SVGPan.js"/>
    <g transform='translate(100,0)'><title>Simulation output</title>'''
        print(head)

    def __exit__(self, *exc_info):
        foot = '''
        </g>
      </svg>
    </div>
  </body>
</html>
        '''
        foot = '''</g></svg>'''
        print(foot)
        
def log_svg_task():
    h = 10
    tasks = set()
    with svg_printer():
        while 1:
            t,cpu,pid,work,kwargs = (yield)
            y = h*pid
            if pid not in tasks:
                tasks.add(pid)
                fmt = "<text font-size='{h}pt' text-anchor='right' x='-100' y='{y}' width='100'>{s}</text>"
                print(fmt.format(h=h, y=y, s=pid))

            title = kwargs.get('title', None)
            if title:
                print('<g><title>t=%.2f: %s</title>' % (t/float(ONE_TICK), title))
            color = kwargs.get('color', None)
            stroke = color or 'black'
            fmt = "<rect x='{x}' y='{y}' width='{w}' height='{h}' stroke='{stroke}' fill='{color}' stroke-width='1'/>"
            print(fmt.format(x=t*10./ONE_TICK, y=y, w=work*10./ONE_TICK,
                             h=h, color=color, stroke=stroke))
            if title:
                print('</g>')
    
_sys_cpu = 0
_sys_now = 1
_sys_sleep = 2
_sys_busy = 3
_sys_getpid = 4
_sys_park = 5
_sys_unpark = 6
_sys_spawn = 7
_sys_exit = 8

# simulator syscalls
def sys_cpu():
    return (yield (_sys_cpu,))
def sys_now():
    return (yield (_sys_now,))
def sys_sleep(delay):
    yield _sys_sleep, delay
def sys_busy(delay, **kwargs):
    yield _sys_busy, delay, kwargs
def sys_getpid():
    return (yield (_sys_getpid,))
def sys_park():
    return (yield (_sys_park,))
def sys_unpark(pid):
    yield _sys_unpark, pid
def sys_spawn(task):
    return (yield _sys_spawn, task)
def sys_exit(rval=0):
    yield _sys_exit, rval

def simulator(init_task, log=log_devnull()):
    '''A discrete event-based simulator allowing any degree of
concurrency. All tasks run exactly when scheduled.

    Simulator state is initialized with 1+ "seed" tasks (all of which
take zero arguments and will be executed at t=0), obtained by calling
init_fn(time_fn, enqueue_fn). The init_fn must accept a dictionary as
input, which will be filled with functions that give access to the
simulator's internal state.

    A "task" consists of the triple (t, fn, args), where t is the time
at which the task should run, fn is the callable to invoke, and args
is the set of arguments to invoke fn with (or the empty tuple if no
args are needed).

    '''
    # simulator loop variables
    task, pid, fn, args, delay, reschedule = (None,)*6
    
    # initialize the log
    next(log)
    def log_send(t, kwargs):
        log.send((now, None, pid, t, kwargs))

    # time-related syscalls
    now = 0
    def sys_now():
        return now
    def sys_sleep(t):
        nonlocal delay
        delay += t
        
    def sys_busy(t, kwargs):
        log_send(t, kwargs)
        sys_sleep(t)

    # three different ways to give control to a task
    task_start = lambda _:    task.send(None)
    task_send  = lambda args: task.send(args)
    task_throw = lambda args: task.throw(*args)

    # create and identify tasks
    todo = []
    pid_maker = itertools.count(1)
    active_tasks = collections.defaultdict(lambda: next(pid_maker))
    def task_activate(task, pid=None, fn=task_start, args=None):
        # A minimal activation delay prevents a new task with a low
        # PID from displacing the current task at todo[0].
        pid = pid or active_tasks[task]
        old = todo[0] if todo else None
        heapq.heappush(todo, (now+1, pid, task, fn, args))
        assert not old or (old == todo[0])
        return pid
    def sys_spawn(task):
        return task_activate(task)
    def sys_getpid():
        return pid

    # park, and unpark tasks        
    class TaskParked(Exception):
        pass

    parked_tasks,unpark_events = {},set()
    def sys_park():
        try:
            unpark_events.remove(pid)
        except KeyError:
            parked_tasks[pid] = (task,now)
            raise TaskParked from None
            
        return 0
            
    def sys_unpark(pid):
        try:
            task,then = parked_tasks.pop(pid)
        except KeyError:
            unpark_events.add(pid)
        else:
            task_activate(task, pid, fn=task_send, args=now-then)
        
    # terminate simulation
    def sys_exit(rval):
        raise StopIteration(rval)

    # initialize simulator's TODO list
    syscalls = [None]*100
    syscalls[_sys_cpu] = lambda:None
    syscalls[_sys_now] = sys_now
    syscalls[_sys_sleep] = sys_sleep
    syscalls[_sys_busy] = sys_busy
    syscalls[_sys_getpid] = sys_getpid
    syscalls[_sys_park] = sys_park
    syscalls[_sys_unpark] = sys_unpark
    syscalls[_sys_spawn] = sys_spawn
    syscalls[_sys_exit] = sys_exit

    # go!
    rval = 0
    task_activate(init_task)
    while todo:
        delay,reschedule = 0,False
        t, pid, task, fn, args = todo[0]
        assert now <= t
        now = t
        try:
            # Pass the task the results of its last syscall, and let
            # it continue to its next syscall (tasks only consume
            # simulated CPU time during syscalls, often sys_busy). If
            # an exception escapes from a task, terminate the
            # simulation on the assumption that we've hit a bug.
            syscall, *args = fn(args)
            
        except StopIteration:
            # task exited; remove from active set and continue
            #errlog('pid %d exited', pid)
            del active_tasks[task]
                
        else:
            try:
                # execute syscall, schedule delayed return
                args = syscalls[syscall](*args)
            except TaskParked:
                # somebody else will unpark the task later
                #errlog('pid %d parked', pid)
                pass
            except StopIteration as si:
                #errlog('pid %d called sys_exit(%s)', pid, si.value)
                rval = si.value
                break
            except:
                err = sys.exc_info()
                #errlog('pid %d: %s(%s) threw %s (%s)', pid, fn, args, err[0], err[1])
                fn, args = task_throw, err
                sys_busy(ONE_TICK, dict(title='Exception', color='red'))
                reschedule = True
            else:
                fn, reschedule = task_send, True

        # now what?
        if not reschedule:
            heapq.heappop(todo)
        elif delay > 0:
            heapq.heapreplace(todo, (now+delay, pid, task, fn, args))
        else:
            todo[0] = (now, pid, task, fn, args)

    if len(parked_tasks):
        errlog('Oops: %d parked tasks at exit:', len(parked_tasks))
        for pid,(task,then) in parked_tasks.items():
            errlog('\tpid=%d since %.2f (%d ticks ago)'
                   % (pid, then/float(ONE_TICK), (now-then)//ONE_TICK))
        
    return rval

def cpu_simulator(ncpu=1):
    assert not 'code rot'
    
    '''A discrete event simulator representing a system with cooperative
    multitasking over some fixed number of processors (ncpu).

    '''
    todo = []
    cpus = [(0,i) for i in xrange(ncpu)]
    task_idle, cpu_idle = 0,0

    # accessors 
    def time():
        return cpus[0][0]
    def task_cpu():
        return cpus[0][1]
    def enqueue(when, task, args=()):
        heapq.heappush(todo, (when, task, args))
    def task_busy(t):
        assert t > 0
        nt = time()+t
        heapq.heapreplace(cpus, (nt, task_cpu()))
        enqueue(nt, 
        

    todo.extend((0, fn, None) for fn in init_fn(time=time,
                                                enqueue=enqueue,
                                                task_cpu=task_cpu,
                                                task_busy=task_busy
                                            )))
    
    heapq.heapify(todo)
    heapq.heapfiy(cpu)
    while todo:
        t, fn, args = heapq.heappop(todo)
        now,cpu = cpu[0]
        if now < t:
            cpu_idle += t - now
            cpu[0][0] = t
        elif t < now:
            task_idle += now - t
            
        fn(args)

def tarjan_vanilla(tx_deps, include_trivial=False):
    '''A vanilla implementation of Tarjan's algorithm.

    This variant is not used directly, but rather serves as a
    reference point for specialized variants we do use.

    '''
    last_index = 0
    def new_index():
        nonlocal last_index
        last_index += 1
        return last_index

    index = collections.defaultdict(new_index)
    def index_seen(dep):
        tmp = last_index
        j = index[dep]
        unseen = (j == last_index and tmp != last_index)
        return j,unseen
    
    low,stack,s,scs = {}, set(), [], []
    
    def connect(pid, deps):
        i = last_index
        ilow = low[i] = i
        s.append(pid)
        stack.add(i)
        for dep in deps:
            j,unseen = index_seen(dep)
            if unseen:
                jlow = connect(dep, tx_deps.get(dep, ()))
                if jlow < ilow:
                    ilow = low[i] = jlow
            elif j in stack and j < ilow:
                ilow = low[i] = j

        if ilow == i:
            sc,dep = [],None
            while pid != dep:
                dep = s.pop()
                stack.remove(index[dep])
                sc.append(dep)

            if len(sc) > 1 or include_trivial:
                scs.append(sc)
                
        return ilow

    for pid,deps in tx_deps.items():
        i,unseen = index_seen(pid)
        if unseen:
            connect(pid, deps)

    return scs

def test_tarjan():
    deps = collections.defaultdict(set)
    deps[1].update({2, 9})
    deps[2].update({3})
    deps[3].update({1})
    deps[4].update({3, 5})
    deps[5].update({4, 6})
    deps[6].update({3, 7})
    deps[7].update({6})
    deps[8].update({5, 8})
    deps[10].update({8})
    scs = tarjan_vanilla(deps, True)
    for sc in scs:
        sc.sort()
        errlog('%s', ' '.join(str(x) for x in sc))


def tarjan_serialize(tx_deps, old_safe_point, new_safe_point, show_all=False):
    '''A variant of Tarjan's algorithm specialized for finding
    serialization failures in the partitioned dependency graph we
    generate. It focuses only on strongly connected components
    "anchored" between old and new safe points, and usually only
    reports SCC of size 2 or more.

    '''
    last_index = 0
    def new_index():
        nonlocal last_index
        last_index += 1
        return last_index

    index = collections.defaultdict(new_index)
    def index_seen(dep):
        tmp = last_index
        j = index[dep]
        unseen = (j == last_index and tmp != last_index)
        return j,unseen
    
    low,stack,s,scs = {}, set(), [], []
    
    def connect(pid, deps):
        i = last_index
        ilow = low[i] = i
        s.append(pid)
        stack.add(i)
        for dep in deps:
            # safe to skip, see long comment in dependency_tracker()
            if new_safe_point <= dep:
                continue
            
            j,unseen = index_seen(dep)
            if unseen:
                jlow = connect(dep, tx_deps.get(dep, ()))
                if jlow < ilow:
                    ilow = low[i] = jlow
            elif j in stack and j < ilow:
                ilow = low[i] = j

        if ilow == i:
            sc,dep,unreported = [],None,False
            while pid != dep:
                dep = s.pop()
                unreported |= old_safe_point <= dep
                stack.remove(index[dep])
                sc.append(dep)
            if unreported and (len(sc) > 1 or show_all):
                scs.append(sc)
                
        return ilow

    new_deps = collections.defaultdict(dict)
    for pid,deps in tx_deps.items():
        if pid < new_safe_point:
            i,unseen = index_seen(pid)
            if unseen:
                connect(pid, deps)

        if old_safe_point <= pid:
            new_deps[pid] = deps

    return new_deps, scs

def tarjan_incycle(tx_deps, who):
    '''A variant of Tarjan's algorithm that only cares about the SCC that
    a given node belongs to. If "who" belongs to a non-trivial SCC,
    return the cluster. Otherwise, return None.

    '''
    last_index = 0
    def new_index():
        nonlocal last_index
        last_index += 1
        return last_index

    index = collections.defaultdict(new_index)
    def index_seen(dep):
        tmp = last_index
        j = index[dep]
        unseen = (j == last_index and tmp != last_index)
        return j,unseen
    
    low,stack,s,scs = {}, set(), [], []

    scc = None
    def connect(pid, deps):
        nonlocal scc
        i = last_index
        ilow = low[i] = i
        s.append(pid)
        stack.add(i)
        for dep in deps:
            j,unseen = index_seen(dep)
            if unseen:
                jlow = connect(dep, tx_deps.get(dep, ()))
                if jlow < ilow:
                    ilow = low[i] = jlow
            elif j in stack and j < ilow:
                ilow = low[i] = j

        if ilow == i:
            sc,dep,found = [],None,False
            while pid != dep:
                dep = s.pop()
                if dep == who:
                    found = True
                stack.remove(index[dep])
                sc.append(dep)

            if found and len(sc) > 1:
                scc = sc
                
        return ilow

    _,unseen = index_seen(who)
    assert unseen
    connect(who, tx_deps[who])
    return scc

class NamedTuple(object):
    def __init__(self, **args):
        self.__dict__.update(args)

def dependency_tracker():
    '''Create a dependency tracker.

    The tracker exports two hook functions: on_access and on_commit.

    on_access(pid, rid, dep) notifies the tracker that transaction
    "pid" has accessed (read or overwritten) the value which
    transaction "dep" wrote to record "rid"; it returns two values:
    the value of the read (larger of the dependency and the current
    safe point, see below), and a boolean indicating isolation status
    (False means there was an isolation failure, where the value read
    was written by a transaction that is still in flight).

    on_finish(pid, committed) notifies the tracker that transaction
    "pid" has finished. If "committed" its dependencies should be
    checked for serialization errors. The checker will perform such
    checks during this or some future invocation of on_finish(), and
    will return a list of failures whenever such a check fails. Checks
    are allowed to occur during any call to on_finish, including when
    reporting a failed transaction.

    Internally, the tracker maintains data structures that protect old
    transactions from repeated checking, so that the cost of checking
    does not grow over time.

    The central concept used is a "safe point" -- the pid of some
    transaction known to have committed before any active transaction
    began. Transactions incur no dependencies when accessing values
    written before the current safe point began. An optimal safe point
    could be maintained using a heap, but would be expensive---heap
    maintenance is O(n lg n)---and would force us to check each
    transaction individually for failures over an ever-growing
    dependency graph. Instead, the system maintains a pending safe
    point as well as a set of "pending" transactions. The pending safe
    point will be installed once all pending transactions have
    finished. The last such transaction to complete is chosen to
    become the next pending safe point, and all currently active
    transactions are added to the pending set.

    There are six kinds of transactions in the dependency graph, based
    on how their lifetime compares with the selection and installation
    of safe points:
                                                      now
                                                       |
                                                       v
    -------------- A --------------- B --------------- C --------------
      |--T1--|          |--T3--|          |--T5--|
               |--T2--|          |--T4--|          |--T6--|

    In the above figure, time flows from left to right and is measured
    in terms of transaction ids, which increase
    monotonically. Installation of safe point A must wait until all
    transactions that coexisted with A complete; B is the last to do
    so, and is chosen to become the next safe point. As before, it
    cannot be installed until all coexisting transactions complete,
    with C being the last to do so. Note that the definition of a safe
    point means that every transaction will see at most one safe point
    installed during its lifetime.

    We choose to perform serialization checks whenever a new safe
    point is installed. In the above figure, suppose that C has just
    committed, allowing us to install safe point B. There is no point
    in checking live transactions (e.g. T6) yet because they could
    still enter a cycle after the check. It's also unpalatable to
    check the set of T4 and T5, because differentiating between T3 and
    T4 depends on commit "time" which is messy in our formulation
    based on transaction starts. Instead, at the time C commits (and B
    is installed) we check all transactions that began between A and
    B. All such are guaranteed to have committed, and most SCC we find
    will not have been reported before.

    At each round, the checker will report failures involving T3, T4,
    T2/T3, T2/T4, T3/T4, and T2/T3/T4. We distinguish T3 from T4 by
    noting that all deps for T3 occur *before* new_safe_point, while
    one or more deps occur *after* new_safe_point in T4.
    
    NOTE: While running Tarjan, we could encounter strongly connected
    components involving T3/T4/T5. However, our formulation of safe
    points disallows any direct dependencies between T3 and T5,
    meaning that T4 must be the common member of 2+ unrelated cycles
    (a figure-eight with T4 at the crossing). Each cycle is a
    different (unrelated) serialization failure, so we are content to
    partition the SCC, reporting the T3/T4 subcomponent now and the
    T4/T5 subcomponent next time; by a similar argument, the T1/T2
    subcomponent of a T1/T2/T3 failure will have been reported last
    time, and we must now report the T2/T3 subcomponent.

    NOTE: The first transaction to commit after system startup also
    discovers an empty pending set, and will do the right thing by
    becoming the new safe point and populating the pending set.

    '''
    active = set()
    safe_point = 0
    
    pending = set()
    pending_safe_point = 0

    # track the set of dependencies for each transaction
    deps = collections.defaultdict(dict)
    #all_deps = {}

    def on_access(pid, rid, dep):
        active.add(pid)
        if dep == pid:
            isolated = True
        elif dep > safe_point:
            deps[pid].setdefault(dep, rid)
            isolated = dep not in active
        else:
            dep = safe_point
            isolated = True
            
        return dep,isolated

    def on_finish(pid, is_commit):
        nonlocal deps, safe_point, pending, pending_safe_point
        active.remove(pid)

        # only remember committed transactions
        if not is_commit:
            deps.pop(pid, None)
        else:
            #all_deps[pid] = deps[pid]
            pass

        pending.discard(pid)
        if pending:
            return ()
            
        # any serialization failures since last safe point?
        deps, problems = tarjan_serialize(deps, safe_point, pending_safe_point)
        #_, all_problems = tarjan(all_deps, 0, 1000**3)
        #for p in all_problems:
        #    sys.stderr.write('scc: %s\n' % (' '.join(map(str, p))))
        
        # advance the safe point
        safe_point = pending_safe_point
        pending_safe_point = pid
        pending = set(active)

        return problems

    return NamedTuple(on_access=on_access, on_finish=on_finish)
        

def make_nocc_db(nrecs=100):
    '''A database model implementing no concurrency control
    whatsoever. All accesses are processed in the order they arrive,
    without blocking, and transactions will experience isolation and
    serialization failures on a regular basis.

    '''
    db = [0]*nrecs
    tracker = dependency_tracker()
    tx_count, acc_count, iso_failures, ser_failures = 0,0,0,0
    def db_access(rid):
        nonlocal acc_count, iso_failures
        pid = yield from sys_getpid()
        val,isolated = tracker.on_access(pid, rid, db[rid])
        if not isolated:
            iso_failures += 1
        acc_count += 1
        return pid, val
            
    def tx_read(rid, for_update=False):
        pid, val = yield from db_access(rid)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (val, rid))
        return val
        
    def tx_write(rid):
        pid, _ = yield from db_access(rid)
        db[rid] = pid
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='db[%s]=%s' % (rid, pid))

    def tx_commit():
        nonlocal tx_count, ser_failures
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        
        tx_count += 1
        pid = yield from sys_getpid()
        cycles = tracker.on_finish(pid, True)
        for cycle in cycles:
            ser_failures += len(cycle)
            errlog('Serialization failure found: %s', ' '.join(map(str, cycle)))
            
    def fini():
        errlog('''
        Stats:

        Total transactions: %d (%d serialization failures)
        Total accesses:     %d (%d isolation failures)''',
               tx_count, ser_failures, acc_count, iso_failures)

    return NamedTuple(nrecs=nrecs, tx_read=tx_read, tx_write=tx_write, tx_commit=tx_commit, fini=fini)

class AbortTransaction(Exception):
    '''Raised whenever a transaction fails.

    The user is responsible to call tx_abort on their database so that
any outstanding changes can be rolled back (otherwise, the transaction
is left hanging, in its faile state, forever).

    '''
    pass
    
class DeadlockDetected(AbortTransaction):
    pass

def make_2pl_db(nrecs=100, verbose=False):
    '''A database model implementing strict two phase locking
    (2PL). Transactions must acquire appropriate locks before
    accessing data, and may block (or even deadlock) when requesting
    locks already held by other transactions. Neither isolation nor
    serialization failures are possible.
    '''
    db = [0]*nrecs
    tracker = dependency_tracker()
    tx_count, tx_failures, ser_failures = 0,0,0
    acc_count, lock_waits, iso_failures = 0,0,0
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

    upgr[S, R, R] = (R, R, S)	# trivial	
    upgr[S, R, U] = (U, U, V)
    upgr[S, R, X] = (P, X, X)
    #upgr[S, U, *] =              illegal
    #upgr[S, X, *] =              illegal
    
    upgr[U, R, R] = (R, R, U)	# trivial
    upgr[U, R, U] = None
    upgr[U, R, X] = None
    upgr[U, U, R] = (U, U, U)	# trivial
    upgr[U, U, U] = (U, U, U)	# trivial
    upgr[U, U, X] = (X, X, X)
    #upgr[U, X, *] =              illegal
    
    upgr[V, R, R] = (R, R, V)	# trivial
    upgr[V, R, U] = None
    upgr[V, R, X] = None
    upgr[V, U, R] = (U, U, V)	# trivial
    upgr[V, U, U] = (U, U, V)	# trivial
    upgr[V, U, X] = (P, X, X)
    #upgr[V, X, *] =              illegal
    
    upgr[P, R, R] = (R, R, P)	# trivial
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
    tx_deps = {}
    empty_set, empty_dict = set(), {}
    
    def lock_acquire(pid, rid, mode):
        now = yield from sys_now()
        must_wait,is_upgrade = False,False
        lock = locks[rid]
        q or e('lock_acquire: t=%.2f pid=%d rid=%d lock=%s/%s mode=%s',
               now/float(ONE_TICK), pid, rid,
               mode_name(lock.m), mode_name(lock.wm), mode_name(mode))
        my_locks = tx_locks[pid]
        req = my_locks.get(rid, None)
        if req:
            # existing request, may need upgrade
            try:
                rmode,gmode,smode = upgr[lock.m, req.m, mode]
            except TypeError:
                q or e('\tUpgrade deadlock detected: lock.m=%s/%s req.m=%s/%s',
                       mode_name(lock.m), mode_name(lock.wm),
                       mode_name(req.m), mode_name(mode))
                histo_add(dlock_histo, 1)
                raise DeadlockDetected() from None

            assert not lock.upgrader
            if rmode == gmode:
                q or e('\tUpgrade from %s to %s (%s) succeeds immediately',
                       mode_name(req.m), mode_name(mode), mode_name(gmode))
                lock.m,req.m = smode,gmode
                return

            # abandon current request and block with a new one
            req.m = N
            req = my_locks[rid] = LockRequest(pid, rmode)

            # mark the lock as upgrading
            if lock.wm == lock.m:
                lock.wm = rmode
            lock.m = rmode
            lock.upgrader = req

            # block, then patch things up
            yield from sys_busy(ONE_TICK//10, color='pink',
                                title='Upgrade: rid=%s lock.m=%s req.m=%s rmode=%s'
                                % (rid, lock.m, req.m, rmode))
            try:
                yield from lock_block(lock, req, set())
            except DeadlockDetected:
                lock.holders.append(req)
                lock.upgrader = None
                raise

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

            # block
            yield from sys_busy(ONE_TICK, color='fuchsia',
                                title='Request: rid=%s lock.m=%s/%s mode=%s'
                                % (rid, mode_name(lock.m), mode_name(lock.wm), mode_name(req.m)))
            try:
                yield from lock_block(lock, req, my_deps)
            except DeadlockDetected:
                req.m = N
                del my_locks[rid]
                raise

    def lock_block(lock, req, my_deps):
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
        my_deps.update(dep.pid for dep in lock.holders if dep.m != N)
        pid = req.pid
        tx_deps[pid] = my_deps
        scc = tarjan_incycle(tx_deps, pid)
        if scc:
            q or e('\tDeadlock detected: %s', ' '.join(map(str, sorted(scc))))
            if 0 and len(scc) == 10:
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
                        
            yield from sys_busy(ONE_TICK, color='red', title='deadlock!')
            histo_add(dlock_histo, len(scc))
            raise DeadlockDetected()

        nonlocal lock_waits
        lock_waits += 1
        q or e('\tblocked (lock is %s/%s, deps: %s)',
               mode_name(lock.m), mode_name(lock.wm),
               ' '.join(map(str, sorted(my_deps))))
        delay = yield from sys_park()
        histo_add(wait_histo, delay)
        q or e('\tpid %d unblocked at t=%.2f (lock is %s)',
               pid, (yield from sys_now())/float(ONE_TICK), mode_name(lock.m))
        del tx_deps[pid]
            

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
                lock.wm = lock.m = supr[lock.m][req.m]
                i += 1
                

        unblock, req = [], lock.upgrader
        if req:
            q or e('\tprocess upgrade request (pid=%s lock.m=%s req.m=%s)',
                   req.pid, mode_name(lock.m), mode_name(req.m))
            lock.wm = supr[lock.wm][req.m]
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
        
        
    def db_access(rid, mode):
        nonlocal acc_count, iso_failures
        pid = yield from sys_getpid()
        yield from lock_acquire(pid, rid, mode)
        val,isolated = tracker.on_access(pid, rid, db[rid])
        if not isolated:
            iso_failures += 1
        acc_count += 1
        return pid, val
            
    def tx_read(rid, for_update=False):
        pid, val = yield from db_access(rid, U if for_update else R)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (val, rid))
        return val
        
    def tx_write(rid):
        pid, val = yield from db_access(rid, X)
        tx_logs[pid].setdefault(rid, val)
        db[rid] = pid
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='db[%s]=%s' % (rid, pid))

    def tx_create(pid):
        in_flight[pid] = yield from sys_now()
        
    def finish(pid, is_commit):
        nonlocal tx_count, ser_failures
        tx_count += 1
        tx_logs.pop(pid, None)
        yield from release_all_locks(pid)
        cycles = tracker.on_finish(pid, is_commit)
        for cycle in cycles:
            ser_failures += len(cycle)
            errlog('Serialization failure found: %s', ' '.join(map(str, cycle)))

        then,now = in_flight.pop(pid), (yield from sys_now())
        histo_add(resp_histo, then-now)

    def tx_commit():
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        pid = yield from sys_getpid()
        yield from finish(pid, True)
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        
    def tx_abort():
        nonlocal tx_failures
        tx_failures += 1
        pid = yield from sys_getpid()
        
        # log rollback is not free...
        for rid,val in tx_logs.get(pid, empty_dict).items():
            assert db[rid] == pid
            db[rid] = val
            yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                                color='lightsteelblue')
            
        yield from finish(pid, False)
            
    def fini():
        if in_flight:
            errlog('\nFound %d live transactions at exit (oldest from tick %.2f)',
                   len(in_flight), min(in_flight.values())/float(ONE_TICK))
            
        deadlocks = tarjan_vanilla(tx_locks)
        if deadlocks:
            errlog('\nDeadlocks found:')
            for sc in deadlocks:
                errlog('\n\t%s', ' '.join(map(str, sc)))
        else:
            errlog('\nNo deadlocks found at exit')

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
            
        errlog('''
        Stats:

        Total transactions: %d (%d aborts, %d serialization failures)
        Total accesses:     %d (%d lock waits, %d isolation failures)''',
               tx_count, tx_failures, ser_failures,
               acc_count, lock_waits, iso_failures)
        histo_print(wait_histo, 'lock wait times')
        histo_print(resp_histo, 'transaction response times')
        histo_print(dlock_histo, 'deadlock sizes (1 = upgrade)', xtime=False)

    return NamedTuple(nrecs=nrecs, tx_create=tx_create,
                      tx_read=tx_read, tx_write=tx_write,
                      tx_commit=tx_commit, tx_abort=tx_abort,
                      fini=fini)


def test_2pl_db():

    R,U,X = 1,2,3
    def test_fini(db):
        yield from sys_sleep(1000*ONE_TICK)
        db.fini()
        yield from sys_exit()

    def access(db, rid, mode, delay):
        yield from sys_sleep(delay*ONE_TICK)
        yield from db.tx_write(rid) if mode == X else db.tx_read(rid, mode == U)

    def commit(db, delay):
        yield from sys_sleep(delay*ONE_TICK)
        yield from db.tx_commit()
        
    def tx_one(db, rid, mode, delay1, delay2):
        def thunk():
            try:
                yield from access(db, rid, mode, delay1)
                yield from commit(db, delay2)
            except AbortTransaction:
                yield from db.tx_abort()
                
        return (yield from sys_spawn(thunk()))
        
    def tx_two(db, rid1, mode1, rid2=None, mode2=None, delay1=0, delay2=0, delay3=0):
        def thunk():
            try:
                yield from access(db, rid1, mode1, delay1)
                yield from access(db, rid2 or rid1, mode2 or mode1, delay2)
                yield from commit(db, delay3)
            except AbortTransaction:
                yield from db.tx_abort()
                
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
        db = make_2pl_db(nrecs=10, verbose=True)
        simulator(test(db), log=log_svg_task())
        
        
    
def make_client(n, db, max_inflight):
    cpid = yield from sys_getpid()

    # track completions separately: a newly-spawned task might
    # complete before the client adds it to the in-flight set.
    in_flight = set()
    completed = set()

    def do_transaction():
        try:
            yield from transaction()
        except AbortTransaction:
            yield from db.tx_abort()

    def transaction():
        yield from sys_busy(ONE_TICK, color='yellow', title='client %d' % n)
        write_ratio = .25
        nsteps = random.randint(8,12)
        nreads = random.randint(int(nsteps - 2*write_ratio*nsteps), nsteps-1)
        for i in range(nsteps):
            sleep = n*random.randint(1, 5)*random.randint(1, ONE_TICK)
            busy = random.randint(ONE_TICK, 2*ONE_TICK)
            yield from sys_sleep(sleep)
            rid = random.randint(0, db.nrecs-1)
            if i < nreads:
                yield from db.tx_read(rid)
            else:
                yield from db.tx_write(rid)
            yield from sys_busy(busy)

        yield from db.tx_commit()

        # notify the client that we completed
        pid = yield from sys_getpid()
        completed.add(pid)

    overloaded = False
    while 1:
        # sleep for some amount of time before submitting another task
        yield from sys_sleep(n*random.randint(50*ONE_TICK, 150*ONE_TICK))
        
        # process completions before declaring overload
        if len(in_flight) == max_inflight and completed:
            in_flight -= completed
            completed.clear()

        # new tasks cannot spawn if overloaded
        if len(in_flight) < max_inflight:
            pid = yield from sys_spawn(do_transaction())
            yield from db.tx_create(pid)
            in_flight.add(pid)
            overloaded = False
        elif 1 and not overloaded:
            overloaded = True
            now = yield from sys_now()
            errlog('t=%.2f: client %d overloaded (%s)',
                   now/float(ONE_TICK), n, ' '.join(map(str, sorted(in_flight))))

            
def make_benchmark(make_db, nclients, max_inflight, nrec=100, duration=2000*ONE_TICK):
    db = make_db(nrec)
    for i in range(nclients):
        yield from sys_spawn(make_client(n=3, db=db, max_inflight=max_inflight))

    yield from sys_sleep(duration)
    db.fini()
    yield from sys_exit()

if 0:
    test_tarjan()
    test_2pl_db()
    exit(0)

try:
    seed = sys.argv[1]
except:
    # 64**8 possible seeds (~ 10**15) should be enough...
    seed_material = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-'
    seed = ''.join(random.choice(seed_material) for i in range(8))

errlog('Using RNG seed: %s', seed)
random.seed(seed)

simulator(make_benchmark(make_db=make_2pl_db, nclients=100, max_inflight=100, nrec=2000, duration=5000*ONE_TICK), log=log_svg_task())
