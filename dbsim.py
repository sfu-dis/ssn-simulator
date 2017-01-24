import argparse, collections, random
from sim import *

dict_of_sets = lambda: collections.defaultdict(set)
dict_of_dicts = lambda: collections.defaultdict(dict)

'''A module for performing database workload simulations.

It provides infrastructure for creating clients and transactions, as
well as automatic detection of isolation and serialization failures.

The simulated database contains a fixed number of records, each of
which contains the PID of the last transaction to update it. The
database model handles the actual storage, but whatever values it
returns are vetted for isolation and serialization errors.

'''

def tarjan(tx_deps, include_trivial=False):
    '''A vanilla implementation of Tarjan's algorithm.

    This variant is not used directly, but rather serves as a
    reference point for specialized variants we do use.

    WARNING: it's pretty easy to max out python's recursion limit if
    you throw a large graph at this function...

    '''
    last_index,index = 0,{}
    def safe_index(dep):
        nonlocal last_index
        j = index.setdefault(dep, last_index+1)
        if j > last_index:
            last_index += 1
            return j,True

        return j,False
    
    low,stack,s,scs = {}, set(), [], []
    
    def connect(pid, i, deps):
        ilow = low[i] = i
        s.append(pid)
        stack.add(i)
        for dep in deps:
            j,unseen = safe_index(dep)
            if unseen:
                jlow = connect(dep, j, tx_deps.get(dep, ()))
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
        i,unseen = safe_index(pid)
        if unseen:
            connect(pid, i, deps)

    return scs

def tarjan_iter(tx_deps, include_trivial=False):
    '''A vanilla implementation of Tarjan's algorithm.

    It turns out to be pretty easy to max out python's recursion limit
    if you throw a large graph at this function, so we cheat and use
    generators to make an iterative algorithm of sorts.

    '''
    last_index,index = 0,{}
    def safe_index(dep):
        nonlocal last_index
        j = index.setdefault(dep, last_index+1)
        if j > last_index:
            last_index += 1
            return j,True

        return j,False
    
    low,stack,s,scs = {}, set(), [], []
    
    def connect(pid, i, deps):
        ilow = low[i] = i
        s.append(pid)
        stack.add(i)
        for dep in deps:
            j,unseen = safe_index(dep)
            if unseen:
                jlow = (yield connect(dep, j, tx_deps.get(dep, ())))
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

    def do_it():
        for pid,deps in tx_deps.items():
            i,unseen = safe_index(pid)
            if unseen:
                yield connect(pid, i, deps)

    todos = [do_it(),None]
    while len(todos) > 1:
        args = todos.pop()
        action = todos[-1]
        try:
            rval = action.send(args)
        except StopIteration as stop:
            # resume the parent generator
            todos[-1] = stop.value
        else:
            # rval contains a sub-generator to exhaust first
            todos.append(rval)
            todos.append(None)
            
    return scs

def get_rids_from_subgraph(tx_deps, tids):
    tids,rids = set(tids),set()
    for tid in tids:
        for dep,(rid,edge) in tx_deps[tid].items():
            if dep in tids:
                rids.add(rid)

    return tids,rids
    
def subgraph_to_dot(out, tx_deps, tids=None, core={}):
    if tids is None:
        tids = set(tx_deps)
        
    out = make_log(out)
    tids,rids = get_rids_from_subgraph(tx_deps, tids)

    out('\ndigraph G {')
    for pid in tids:
        if pid in core:
            out('\t"T%d" [ color="red" ]', pid)
            
        deps = tx_deps.get(pid,{})
        for dep,(rid,tp) in deps.items():
            x,y = tp.split(':')
            if dep in tids:
                out('\t"T%d" -> "T%s" [ label="%s:R%s:%s" ]', pid, dep, y, rid, x)
    errlog('}\n')

                    


def test_tarjan():
    deps = {
        1: {2, 9},
        2: {3},
        3: {1},
        4: {3, 5},
        5: {4, 6},
        6: {3, 7},
        7: {6},
        8: {5, 8},
        10: {8},
    }
    scs = tarjan_iter(deps, True)
    for sc in scs:
        sc.sort()
        errlog('%s', ' '.join(str(x) for x in sc))


def tarjan_incycle(tx_deps, who):
    '''A variant of Tarjan's algorithm that only cares about the SCC that
    a given node belongs to. If "who" belongs to a non-trivial SCC,
    return the cluster. Otherwise, return None.

    '''
    last_index,index = 0,{}
    def safe_index(dep):
        nonlocal last_index
        j = index.setdefault(dep, last_index+1)
        if j > last_index:
            last_index += 1
            return j,True

        return j,False
            
    low,stack,s,scs = {}, set(), [], []

    scc = None
    def connect(pid, i, deps):
        nonlocal scc
        ilow = low[i] = i
        s.append(pid)
        stack.add(i)
        for dep in deps:
            j,unseen = safe_index(dep)
            if unseen:
                jlow = connect(dep, j, tx_deps.get(dep, ()))
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

    i,unseen = safe_index(who)
    assert unseen
    connect(who, i, tx_deps[who])
    return scc

class NamedTuple(object):
    def __init__(self, *args, **kwargs):
        '''Initialize a new NamedTuple by merging in zero or more existing
        NamedTuple objects and adding zero or more additional
        properties. Arguments are processed from left to right, with
        last updater taking precedence in case of a naming collision.

        '''
        for arg in args:
            self.__dict__.update(arg.__dict__)
            
        self.__dict__.update(kwargs)

def timestamp_generator(start=1):
    next_stamp = start
    def thunk():
        nonlocal next_stamp
        rval,next_stamp = next_stamp,next_stamp+1
        return rval

    return thunk
    
def tx_tracker(stats, admission_limit, **extra_kwargs):
    '''Infrastructure to track in-flight transactions. Tracking can be
    enabled and disabled to extract accurate response time
    measurements for transactions that begin during a time window of
    interest in a continuous stream of transactions.

    Note that a transaction should not be reported as "finished" until
    the system is ready to forget it completely. In particular, a
    transaction cannot finish before any deferred checks for
    serialization failures that might be required.

    '''
    # known[pid] = (begin_stamp, end_stamp, is_commit)
    end_unknown,commit_unknown = None,None
    tx_known,tx_active  = {0:(1,1,True)},set()
    
    tracking_active,tracking_ending = False,False
    tx_tracked,tracking_callback = set(),None
    get_next_stamp = timestamp_generator(2)
    
    stats.tx_count,stats.tx_commits,stats.tx_aborts = 0,0,0
    
    def begin_tracking():
        '''Begin a measurement window.

        During a measurement, all transaction arrivals and completions
        are counted in order to determine throughput. Further, any
        transaction which starts while tracking is active will affect
        statistics (isolation, serialization, failures, etc.) and will
        be followed to completion even if tracking is later disabled
        (to measure latency accurately).

        It is an error to begin a new measurement before the previous
        one completes (including completion of all stragglers).

        '''
        nonlocal tracking_active
        assert not tracking_active and not tracking_ending
        tracking_active = True

    def end_tracking(callback=None):
        '''Notify the tracker that a measurement window has closed. All
        in-flight transactions will be followed to completion, but no
        new transactions will be tracked.

        The optional callback will be invoked after the last straggler
        completes and has been checked for serialization failures.

        '''
        nonlocal tracking_active,tracking_ending,tracking_callback
        assert tracking_active
        tracking_active,tracking_ending = False,True
        tracking_callback = callback

    def on_begin(pid):
        '''Notify the tracker of a new transaction, returning its begin
        timestamp and whether it is tracked (if the latter is true, it
        is part of a measurement and will be followed to completion
        even if the measurement window closes first).

        If admission control is active and too many transactions are
        already in flight, raise an AdmissionControl exception;
        completed but unfinalized transactions do not count towards
        the admission control limit.

        '''
        tx_active.add(pid)
        begin = get_next_stamp()
        tx_known[pid] = (begin, end_unknown, commit_unknown)
        if tracking_active:
            tx_tracked.add(pid)
            
        if admission_limit and len(tx_active) > admission_limit:
            raise AdmissionControl

        return begin,tracking_active

    tx_precommit = {}
    def on_precommit(pid):
        '''Notify the tracker that a transaction is attempting to
        complete. The returned timestamp will become the transaction's
        completion time if it commits.

        '''
        return tx_precommit.setdefault(pid, get_next_stamp())
        
    def on_finish(pid, is_commit):
        '''Notify the tracker that a transaction has completed (abort or
        commit, it doesn't matter). Return the transaction's end
        timestamp and whether it was tracked.

        The transaction will no longer impact admission control, but
        will still be tracked until explicitly laid to rest by a call
        to on_finalize().

        '''
        tracked = is_tracked(pid)
        if tracked:
            stats.tx_count += 1
            if is_commit:
                stats.tx_commits += 1
            else:
                stats.tx_aborts += 1

        tx_active.remove(pid)
        begin,_,_ = tx_known[pid]
        end = tx_precommit.pop(pid, None) or get_next_stamp()
        tx_known[pid] = (begin, end, is_commit)
        return end,tracked

    def is_known(pid):
        '''Return True if the system knows about the given PID. False
        indicates the PID is either unknown, or has already been
        finalized and forgotten.

        As a special case, PID=0 is permanently known as an invalid
        transaction which committed at t=1.

        '''
        return pid in tx_known or not pid

    def is_tracked(pid):
        '''Return True if the transaction is known to be tracked.

        '''
        return pid in tx_tracked
        
    def is_committed(pid):

        '''Return the commit timestamp of a transaction (always non-zero) if
        it is known to have committed. Return False if the transaction
        is known to have aborted, and None if still in flight.

        It is an error to call this function for an unknown PID.

        '''
        _,end,is_commit = tx_known[pid]
        return is_commit and end

    def get_begin(pid):
        '''Return the begin time of a known transaction'''
        begin,_,_ = tx_known[pid]
        return begin
        
    def get_end(pid):
        '''Return the end time of a known transaction. You probably want to
        use is_committed() instead.'''
        
        _,end,_ = tx_known[pid]
        return end

    def on_finalize(pid):
        '''Notify the tracker that a transaction has left the system entirely
        and can be forgotten. If it was the last tracked transaction,
        invoke the tracking callback (if any).

        '''
        nonlocal tracking_ending, tracking_callback
        assert pid not in tx_precommit
        tx_tracked.discard(pid)
        tx_known.pop(pid)
        
        if tracking_ending and not tx_tracked:
            cb,tracking_ending,tracking_callback = tracking_callback,False,None
            cb and cb()
            

    def get_stragglers():
        '''Return the set of tracked transactions that prevents the simulation
        from ending (if tracking is still active, the set is empty).

        '''
        return iter(tx_tracked) if tracking_ending else ()

    return NamedTuple(
        begin_tracking=begin_tracking,
        end_tracking=end_tracking,
        is_tracking_active=lambda: tracking_active,
        is_tracking_ending=lambda: tracking_ending,
        get_stragglers=get_stragglers,
        get_next_stamp=get_next_stamp,
        
        is_known=is_known,
        is_tracked=is_tracked,
        is_committed=is_committed,
        get_begin=get_begin,
        get_end=get_end,
        on_begin=on_begin,
        on_precommit=on_precommit,
        on_finish=on_finish,
        on_finalize=on_finalize
    )
            
def historic_log():
    '''Any real implementation is rather limited in what it can track at
    runtime, which makes it difficult or impossible to reconstruct
    history when something goes wrong. In particular, it is impossible
    to detect cycles with 100% accuracy and precision (no false
    positives or negatives) unless the entire history has been stored.

    The reason cycles are hard to detect is that a chain of
    anti-dependencies could form as follows:

    T{i} r:w ... r:W T1 w:r T{i}, with transactions numbered by the
    order they commit. This is true even if we limit how long an old
    version is available once an overwrite commits. As long as any
    write by T1 remains visible (not overwritten), T{i} could close a
    cycle simply by accessing it.

    In order to provide reliable cycle detection and reconstruction of
    history, without allowing implementations to "cheat", we log all
    events (read, write, commit) to this module, and request cycle
    detection and other services only once the simulation
    completes. Implementations can then track as much or as little
    information as they want, without giving up diagnostic capability.

    '''
    get_next_stamp = timestamp_generator()

    # tx_reads[pid] = (rid,dep,when)
    tx_reads,tx_writes = dict_of_sets(),dict_of_sets()

    # tx_endings[pid] = (begin,commit,abort)
    tx_tracked = set()
    tx_events = {0:(1,1,None)}
    
    R,W,C = 'read', 'overwrite', 'commit'

    def on_begin(pid, is_tracked):
        if is_tracked:
            tx_tracked.add(pid)
        when = get_next_stamp()
        tx_events[pid] = (when,None,None)

    def on_access(pid, rid, dep, is_read):
        when = get_next_stamp()
        who = tx_reads if is_read else tx_writes
        who[pid].add((rid,dep,when))

    def on_finish(pid, is_commit):
        when = get_next_stamp()
        begin,c,a = tx_events[pid]
        if is_commit:
            c = when
        else:
            a = when
            
        tx_events[pid] = (begin,c,a)

    def get_tx_deps(include_aborts=False, include_inflight=False):
        '''Build a consolidated dependency graph, with at most one edge
        joining any pair of transactions in each direction.

        The graph will normally include only committed transactions
        and edges between them, but callers can request to include
        aborted and/or in-flight transactions as well.

        The graph is suitable for running tarjan(), and includes the
        necessary information to reconstruct swim lanes on the
        resulting strongly connected components as well.

        '''
        tx_deps,clobbers = dict_of_dicts(),{}
        if include_aborts:
            if include_inflight:
                # can abort, can be in flight
                is_valid = lambda pid: True
            else:
                # can abort, but must not in flight
                is_valid = lambda pid: tx_events[pid][1:3] != (None,None)
        else:
            if include_inflight:
                # no aborts, can be in flight
                is_valid = lambda pid: tx_events[pid][2] is None
            else:
                # must have commited
                is_valid = lambda pid: tx_events[pid][1] is not None

        for pid,writes in tx_writes.items():
            if is_valid(pid):
                for rid,dep,when in writes:
                    if pid != dep and is_valid(dep):
                        #errlog('%d w:%d:w %d', dep,rid,pid)
                        tx_deps[pid].setdefault(dep, (rid,'w:w')) # dep w:w pid
                        clobbers[rid,dep] = pid
                        
        for pid,reads in tx_reads.items():
            if is_valid(pid):
                for rid,dep,when in reads:
                    if pid != dep and is_valid(dep):
                        #errlog('%d w:%d:r %d', dep,rid,pid)
                        tx_deps[pid].setdefault(dep, (rid,'w:r')) # dep w:r pid
                        clobber = clobbers.get((rid,dep),None)
                        if clobber:
                            #errlog('%d r:%d:w %d', pid,rid,clobber)
                            tx_deps[clobber].setdefault(pid,(rid,'r:w')) # pid r:w clobber

        return tx_deps

    def get_neighborhood(tx_deps, tid):
        '''Return the subset of tx_deps that involves any peer of the given
        tid.

        '''
        # find the time range we care about
        first_begin,c,a = tx_events[tid]
        last_end = a or c

        # find the transactions whose lifetime overlaps 
        peers = set()
        for dep in tx_deps:
            begin,c,a = tx_events[dep]
            end = a or c
            if end < first_begin:
                pass
            elif last_end < begin:
                pass
            else:
            #if not (end < first_begin or last_end < begin):
                peers.add(dep)

        assert tid in peers

        # find transactions that tid depends on
        changed = False
        connected = set()
        def add(tid):
            nonlocal changed
            if tid not in connected:
                changed = True
                errlog('adding %d', tid)
                connected.add(tid)
                for dep in tx_deps.get(tid,()):
                    add(dep)

        add(tid)
        while changed:
            changed = False
            for dep,xdeps in tx_deps.items():
                if dep in connected:
                    continue
                for x in xdeps:
                    if x in connected:
                        changed = True
                        connected.add(dep)
                        break

        peers = connected
        assert tid in peers

        # now build the sub-graph
        result = {}
        for tid,deps in tx_deps.items():
            if tid not in peers:
                continue
                
            deps = {dep:edge for dep,edge in deps.items() if dep in peers}
            if deps:
                result[tid] = deps

        return result
        
    def print_swimlane(out, tx_deps, tids):
        out = make_log(out)
        tids,rids = get_rids_from_subgraph(tx_deps, tids)

        # print header row
        out('%s', '\t\t'.join('T%d' % (i+1) for i in range(len(tids))))

        # collect relevant reads and writes from all transactions
        R,W,C,A = 'read', 'overwrite', 'commit', 'abort'
        events = set()
        for tid in tids:
            try:
                begin,c,a = tx_events[tid]
            except KeyError:
                pass
            else:
                events.add((begin,tid,'begin',None,None))
                if c is not None:
                    events.add((c,tid,'commit',None,None))
                if a is not None:
                    events.add((a,tid,'abort',None,None))

            for rid,dep,when in tx_reads.get(tid,()):
                if rid in rids:
                    events.add((when,tid,R,rid,dep))
            for rid,dep,when in tx_writes.get(tid,()):
                if rid in rids:
                    events.add((when,tid,W,rid,dep))

        # print events
        events = sorted(events)
        tmap = collections.defaultdict(timestamp_generator(1))
        rmap = collections.defaultdict(timestamp_generator(1))
        for when,tid,action,rid,dep in events:
            tnum,rnum = tmap[tid],rmap[rid]
            indent = '\t\t'*(tnum-1)
            if rid is None:
                # commit
                out('%s%s', indent, action)
            elif dep not in tids:
                # read or write to baseline data
                out('%s%s R%d', indent, action, rnum)
            else:
                # read or write to data produced in the cycle
                out('%s%s R%d/T%d', indent, action, rnum, tmap[dep])

        tmp = sorted((a,b) for b,a in tmap.items())
        out('%s', '\t\t'.join('T%d' % pid for (i,pid) in tmp))
        
        
    def get_lifetimes(tids):
        '''For each tid on the list, return whetherit committed, whether it
        was tracked, and when it ended.

        '''
        return {tid:tx_events[tid] for tid in tids if tid in tx_events}

    def is_tracked(tid):
        return tid in tx_tracked

    return NamedTuple(on_begin=on_begin,
                      on_access=on_access,
                      on_finish=on_finish,
                      get_tx_deps=get_tx_deps,
                      get_lifetimes=get_lifetimes,
                      get_neighborhood=get_neighborhood,
                      is_tracked=is_tracked,
                      print_swimlane=print_swimlane)
                      

class SerializationFailure(Exception): pass

def safe_dependency_tracker(stats,
                            db_size,
                            ww_blocks,
                            wr_blocks,
                            allow_cycles,
                            report_cycles,
                            tid_watch,
                            rid_watch,
                            analyze_tids,
                            verbose,
                            **extra_kwargs):
    '''Create a epoch-based multi-version dependency tracker.

    The tracker partitions time into a series of epochs, as follows:

    Every epoch begins when some transaction ends.

    Whenever an epoch begins, the ending timestamp and set of
    in-flight transactions are recorded; the timestamp will open the
    next epoch once all in-flight transactions end.

    By construction, no transaction can span more than two epochs,
    giving a hard limit to how far back in time we need to go in order
    to detect all cycles. A version V1 has the following lifecycle, in
    terms of epochs:

    Transaction T1 commits V1 some time during E1. V0 (the version V1
    replaces) is still available for new and existing transactions,
    however. We say that T1 is "new."

    Once E2 arrives, we can forbid any new transactions from accessing
    V0. However, some of T1's peers from E1 could still be in flight,
    and they might still need V0 (SI requires it, for example). T1 and
    V0 are thus "cooling off" during E2.

    Once E3 arrives, All transactions that were using V0 must have
    ended, and we can safely delete V0. If T1 is involved in a cycle,
    other transactions from the cycle could come from E0 (T1 committed
    in E1 but could have started during E0), and could also come from
    E2 (those that started in E1 and committed in E2).  that cycle
    must either be in We cannot forget about T1 quite yet, though, due
    to the following scenario:

    T3(E2/E3) r:w T2(E1/E2) r:w T1(E0/E1) r:w T3

    In the above case, T2 cannot see V1, because it started before T1
    committed. Further, T3 cannot see T2's writes for the same
    reason. However, T3 can most definitely see T1's writes, closing a
    cycle that spans three epochs. Therefore, we continue to remember
    T1, even though it is "cold."

    Once E4 arrives, we can finally detect all cycles that T1 might be
    involved in, using edges from E1/E2/E3.

    A transaction that started in E1 could acquire an anti-dependency on T1

    each ending when all in-flight transactions from the 

    The tracker maintains an internal dependency graph in order to
    detect committed cycles, but does not use that graph to decide
    when to finalize transactions. Instead, the tracker maintains a
    "safe point." No transaction that commits before a safe point will
    carry any dependencies on any transaction that commits after. As
    each new safe point is installed, the tracker discards the
    corresponding subset of the dependency graph, ensuring that the
    cost of cycle checking stays consistent over time.

    The tracker manages the lifecycle and statistics for a simulated
    database workload, and provides an independent means for detecting
    isolation and serialization failures.

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
    # A database record is a list of versions, newest left-most. The
    # only information we actually track is the tid of each version's
    # creator. An old version can be reclaimed once the transaction
    # that overwrites it has been finalized.
    original_version = (0,1)
    db = [collections.deque([original_version]) for i in range(db_size)]
    
    tracker = tx_tracker(stats, **extra_kwargs)
    log = historic_log()
    safe_point = 0

    # install next safe point when all pending transactions end
    active,pending = set(),set()
    pending_safe_point = 1

    # active_* -> in flight accesses
    active_reads, active_writes = dict_of_sets(), dict_of_sets()
    
    # tx_* -> committed accesses
    tx_reads,tx_writes = {},{}
    
    # fresh_tx committed during current safe point; we can safely
    # discard aging_tx at next safe point
    fresh_tx,aging_tx = {},{}

    stats.iso_failures,stats.ser_failures = 0,0
    stats.acc_count,stats.rcount,stats.wcount = 0,0,0
    stats.ww_waits,stats.wr_waits = 0,0

    q,e = not verbose, errlog
    def on_begin(pid):
        begin,tracked = tracker.on_begin(pid)
        active.add(pid)
        log.on_begin(pid, tracked)
        return begin

    clobbering,blocked,no_clobber = {},set(),(None,None)
    def on_access(pid, rid, read):
        '''Notify the tracker that transaction "pid" is accessing record
        "rid", and whether that access is a read or a write. Return
        the value that was read or overwritten.

        If bool(read) is False, a write is assumed and the most
        recently committed version will be overwritten (writes are
        coherent); otherwise, a read is performed. Transactions may
        also provide a callable "read", if they wish to select a
        specific version out of the ones currently available. 

        The version selector callback should accept a single argument,
        an iterator over the triple (id, tid, stamp), where "id" is an
        opaque value identifying the version, "tid" identifies the
        transaction that created the version, and "stamp" is the
        version's commit timestamp. Versions are presented
        newest-first, with a transaction's own write (if any)
        preceding all committed versions (and having tid=pid and
        stamp=None). The callback should either return the id of the
        version it wishes to read, or throw an instance of
        AbortTransaction if no suitable version was available.

        WARNING: allowing a transaction to read two different versions
        (perhaps because a new version became available), or read and
        overwrite different versions, introduces a dependency
        cycle. Transactions can safely read a version they overwrite,
        however, even after issuing the write (though by default the
        transaction will read its own writes).

        '''
        Q = (q and pid not in tid_watch and rid not in rid_watch)
        
        if tracker.is_tracked(pid):
            stats.acc_count += 1
            if read:
                stats.rcount += 1
            else:
                stats.wcount += 1
            
        versions = db[rid]

        def iter_versions():
            dep,stamp = versions[0]
            #e('%d D=%d S=%d', rid, dep, stamp)
            assert (not tracker.is_known(dep)
                    or stamp == tracker.get_end(dep))
            if dep == pid or stamp is not None:
                #e('A %d D=%d S=%d', rid, dep, stamp)
                yield 0,dep,stamp
            
            #e('len(ver)=%d', len(versions))
            for i in range(1,len(versions)):
                dep,stamp = versions[i]
                #e('R%d i=%d D=%d S=%d', rid, i, dep, stamp)
                assert (not tracker.is_known(dep)
                        or stamp == tracker.is_committed(dep))
                yield i,dep,stamp

                # versions become unavailable once their overwrite finalizes
                if not tracker.is_known(dep):
                    #e('%d not known', dep)
                    break

        if read:
            if callable(read):
                # caller wants to select the version to use
                dep,stamp = versions[read(iter_versions())]
                #e('DEP=%d %s', dep, stamp)
            elif wr_blocks and rid in clobbering:
                # uncommitted overwrite, must block
                c,w,rlist = clobbering[rid]
                if c in blocked:
                    raise WaitDepth
                rval = []
                stats.wr_waits += 1
                rlist.append((pid,rval))
                blocked.add(pid)
                yield from sys_park()
                assert len(rval)
                dep,stamp = rval[0]
                #e('DEP1=%d', dep)
            else:
                # use most recently-committed version
                _,dep,stamp = next(iter_versions())
                #e('DEP2=%d', dep)
            
        else:
            # write
            i,dep,stamp = next(iter_versions())
            if not i:
                if dep != pid:
                    Q or e('\tpid=%d taking empty clobber slot for rid=%d', pid, rid)
                    assert rid not in clobbering
                    if ww_blocks:
                        clobbering[rid] = (pid,None,[])

                    active_writes[pid].add((rid,dep))
                    versions.appendleft((pid,None))
                
            elif not ww_blocks:
                raise WWConflict
            else:
                c,w,rlist = clobbering[rid]
                if w or (c in blocked):
                    raise WaitDepth

                clobbering[rid] = (c,pid,rlist)
                Q or e('\tpid=%d blocked on WW conflict on rid=%d with pid=%d', pid, rid, c)
                stats.ww_waits += 1
                blocked.add(pid)
                yield from sys_park()

                assert clobbering[rid][0] is pid
                assert versions[0] == (pid,None)
                dep,stamp = versions[1]
                active_writes[pid].add((rid,dep))

        Q or e('T=%d %s version of R=%d created by X=%d',
               pid, 'reads' if read else 'overwrites', rid, dep)

        log.on_access(pid, rid, dep, read)
        return dep,stamp

    def on_finish(pid, is_commit, callback=None):
        '''Notify the tracker that transaction "pid" has finished, and whether
        it committed or aborted. Return the commit timestamp.

        Once a transaction has committed it cannot roll back and can
        no longer access the database. Other transactions can access
        its updates without triggering isolation failures. It will be
        checked for serialization failures as well.

        '''
        nonlocal safe_point, pending_safe_point, pending
        nonlocal active, fresh_tx, aging_tx

        end,tracked = tracker.on_finish(pid, is_commit)
        log.on_finish(pid, is_commit)
        Q = q and pid not in tid_watch
        Q or e('T=%d %s at %s', pid, 'commits' if is_commit else 'aborts', end)
       
        writes = active_writes.pop(pid,())
        for rid,dep in writes:
            versions = db[rid]
            assert versions[0] == (pid,None)
            assert versions[1][0] == dep
            
            if is_commit:
                # update timestamp of the new version
                versions[0] = (pid,end)
            else:
                # discard it
                versions.popleft()

            if ww_blocks:
                c,w,rlist = clobbering.pop(rid)
                assert c == pid
                for r,rval in rlist:
                    X = Q and r not in tid_watch
                    X or e('\tUnblocking pid=%s from rid=%s', r, rid)
                    rval.append(versions[0])
                    blocked.remove(r)
                    yield from sys_unpark(r)
                    
                if w:
                    X = Q and w not in tid_watch
                    X or e('\tUnblocking pid=%s from rid=%s', w, rid)
                    clobbering[rid] = (w,None,[])
                    versions.appendleft((w,None))
                    blocked.remove(w)
                    yield from sys_unpark(w)
                else:
                    Q or e('\tNobody waiting on rid=%d', rid)
                
        fresh_tx[pid] = callback

        active.discard(pid)
        pending.discard(pid)
        if pending:
            return end

        # advance the safe point
        X = q and not (tid_watch and (aging_tx.keys() & tid_watch))
        X or e('New safe point %d installed when T=%d ended. Forget transactions: %s',
               pending_safe_point, pid, ' '.join(map(str, sorted(aging_tx))))

        for d,cb in aging_tx.items():
            tracker.on_finalize(d)
            cb and cb(d)

        safe_point,pending_safe_point = pending_safe_point,end
        active,pending = set(),active
        aging_tx,fresh_tx = fresh_tx,{}

        # check for stragglers
        active_stragglers = set()
        finished_stragglers = 0
        n = 0
        for d in tracker.get_stragglers():
            n += 1
            commit = tracker.is_committed(d)
            if commit is None:
                active_stragglers.add(d)
            elif d in fresh_tx:
                finished_stragglers += 1

        if n:
            errlog('Safe point %d has %d stragglers, %d finished and %d of them active: %s',
                   safe_point, n, finished_stragglers, len(active_stragglers),
                   ' '.join(map(str, sorted(active_stragglers))))

        return end

    def analyze_cycle(tx_deps, problem):
        '''Apply the proof-guided test to a strongly connected component in
        order to estimate (upper bound) how many transactions are
        responsible for the cycles it contains. This is somewhat more
        fair than reporting the whole SCC as a serialization failure.

        The rule is: starting with the oldest transaction, propagate
        the s(T) to all predecessors who committed later. Flag any
        transaction whose p(T) is not smaller than its s(T).

        To save time and reduce false positives, only follow edges to
        transactions in the SCC.

        '''
        problem = set(problem)
        endings = {pid:(a or c)
                   for pid,(_,c,a) in log.get_lifetimes(problem).items()}
        
        # first, build up s(T)
        smap = {}
        pmap = {}
        for when,pid in sorted((endings[pid],pid) for pid in problem):
            # scc -> all members have at least one relevant dep 
            end = endings[pid]
            preds = pmap[pid] = [x for x in tx_deps[pid] if x in problem]
            s = smap.get(pid, end)
            for x in preds:
                if end < endings[x]:
                    xs = smap.setdefault(x,s)
                    if s < xs:
                        smap[x] = s

        # now, flag the baddies
        core = set()
        for pid in problem:
            s = smap.get(pid, None)
            if s:
                end = endings[pid]
                p = None
                for x in pmap[pid]:
                    xend = endings[x]
                    if x in problem and xend < end:
                        if not p or p < xend:
                            p = xend

                if p and not (p < s):
                    core.add(pid)

        return core
        
        
    def end_tracking(callback=None):
        def thunk():
            errlog(' -*- DONE -*- ')
            tx_deps = log.get_tx_deps()
            problems = tarjan_iter(tx_deps)
            for problem in problems:
                core = analyze_cycle(tx_deps, problem)
                stats.ser_failures += sum(1 for pid in core if log.is_tracked(pid))
                if report_cycles:
                    if len(core) < 6 or report_cycles > 1:
                        last,dots = len(core),''
                    else:
                        last,dots = 5,'...'
                    errlog('Serial failures(s) found (size %d/%d): %s%s',
                           len(core), len(problem),
                           ' '.join(map(str,sorted(core)[0:last])), dots)
                    if report_cycles > 1:
                        if len(problem) < 7:
                            log.print_swimlane(sys.stderr, tx_deps, problem)

                        subgraph_to_dot(sys.stderr, tx_deps, problem, core)

            if problems and not allow_cycles:
                raise SerializationFailure

            if analyze_tids:
                tx_deps = log.get_tx_deps(include_aborts=True)
                
            for tid in analyze_tids:
                subgraph = log.get_neighborhood(tx_deps, tid)
                subgraph_to_dot(sys.stderr, subgraph)
                
            callback and callback()
            
        tracker.end_tracking(thunk)

    def get_overwriter(rid, dep, even_if_inflight=False):
        '''Return the TID of a transaction that committed an overwrite to the
        specified version. If no overwrite has committed, return None.

        If even_if_inflight=True, then in-flight overwrites will be
        reported as well.

        '''
        versions = db[rid]
        who,when = versions[0]
        if who == dep:
            return None,None
            
        for i in range(1,len(versions)):
            if versions[i][0] == dep:
                if even_if_inflight or when is not None:
                    return who,when
                return None,None

            who,when = versions[i]

        errlog('Yikes! cannot find dep=%s in versions of rid=%s: %s',
               dep, rid, versions)
        assert not 'reachable'

    return NamedTuple(begin_tracking=tracker.begin_tracking,
                      end_tracking=end_tracking,
                      is_known=tracker.is_known,
                      is_tracked=tracker.is_tracked,
                      is_committed=tracker.is_committed,
                      get_overwriter=get_overwriter,
                      get_begin=tracker.get_begin,
                      get_end=tracker.get_end,
                      get_safe_point=lambda: safe_point,
                      on_begin=on_begin,
                      on_access=on_access,
                      get_next_stamp=tracker.get_next_stamp,
                      on_precommit=tracker.on_precommit,
                      on_finish=on_finish)

def safe_snap_maker(tracker, safe_snaps):
    assert safe_snaps >= 0
    
    read_only_tx = {}
    last_snap, last_snap_when = 0,0
    next_snap_wait = []
    
    def request_safe_snap(pid):
        nonlocal last_snap, last_snap_when, next_snap_wait
        if not safe_snaps:
            return
            
        next_snap_wait.append(pid)
        if len(next_snap_wait) > 1:
            # safe snap was already pending
            yield from sys_park()
        else:
            # create a new safe snap
            yield from sys_sleep_until(last_snap_when + 1000*safe_snaps)
            last_snap = tracker.get_next_stamp()
            last_snap_when = yield from sys_now()
            for p in next_snap_wait[1:]:
                yield from sys_unpark(p)
            next_snap_wait.clear()

        read_only_tx[pid] = last_snap

    def get_safe_snap(pid):
        return read_only_tx.get(pid, None)
        
    def forget(pid):
        read_only_tx.pop(pid, None)
        
    return NamedTuple(request_safe_snap=request_safe_snap,
                      get_snap=get_safe_snap,
                      get_last_snap=lambda: last_snap,
                      forget=forget,
                  )
                      
    
class AbortTransaction(Exception):
    '''Unspecified failure

    Raise this error whenever a transaction fails and should abort. The
    simulator will catch it and call the database's tx_abort in order
    to roll back any outstanding changes and then invoke the tracker's
    on_finish handler.

    Raising a subclass of this exception allows the simulator to break
    failures down by cause; the first line of the exception class doc
    string acts as its title.

    '''
    pass

class SafeSnapKill(AbortTransaction):
    '''Killed by safe snapshot'''
    
class AdmissionControl(AbortTransaction):
    '''Killed by admission control'''

class TransactionTimeout(AbortTransaction):
    '''Timed out'''

class WWConflict(AbortTransaction):
    '''WW conflict detected'''

class WaitDepth(AbortTransaction):
    '''Cannot wait on a blocked transaction'''


def print_general_stats(stats):
    errlog('''
    Stats:

    Total transactions: %6d (%d commits, %d serialization failures)
    Total accesses:     %6d (%d reads, %d writes, %d isolation failures)
    Blocking events:    %6d (%d W-W, %d W-R)''',
           stats.tx_count, stats.tx_commits, stats.ser_failures,
           stats.acc_count, stats.rcount, stats.wcount, stats.iso_failures,
           stats.ww_waits+stats.wr_waits, stats.ww_waits, stats.wr_waits)
    
def print_failure_causes(stats):
    try:
        fc = stats.failure_causes
    except AttributeError:
        errlog('\nTransaction failure causes not tracked')
        return

    classes = set(n for n,e in stats.failure_causes)

    # always report certain numbers, even if zero
    for name in classes:
        stats.failure_causes[name,Total] += 0
        stats.failure_causes[name,Success] += 0
        stats.failure_causes[name,Failure] += 0
        
    data = sorted((name, n, e.__doc__ and e.__doc__.splitlines()[0] or e.__name__)
                  for (name,e),n in stats.failure_causes.items())
    errlog('\n\tBreakdown of transaction outcomes:')
    for name,n,reason in reversed(data):
        if len(classes) > 1:
            errlog('\t\t%5d: %s: %s', n, name, reason)
        else:
            errlog('\t\t%5d: %s', n, reason)
    
def make_generator(fn):
    def generator(*args):
        if 0:
            yield
        fn(*args)
    return generator
    
def make_nocc_db(tracker, stats, db_size, **extra_kwargs):
    '''A database model implementing no concurrency control
    whatsoever. All accesses are processed in the order they arrive,
    without blocking, and transactions will experience isolation and
    serialization failures on a regular basis.

    As a test for the simulator's abort-handling machinery, writes
    fail with the probability given by "abort_rate." The database
    model's tx_abort() does little more than notify the tracker that
    the transaction completed, however (rollback is not implemented).

    '''
    # spice things up a bit
    abort_rate = 0.000 
        
    def db_access(pid, rid, is_read):
        if abort_rate and random.random() < abort_rate:
            raise AbortTransaction
            
        dep,_ = yield from tracker.on_access(pid, rid, is_read)
        return dep
            
    def tx_read(pid, rid, for_update=False):
        dep = yield from db_access(pid, rid, True)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (dep, rid))
        return dep

    def tx_write(pid, rid):
        dep = yield from db_access(pid, rid, False)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='db[%s]=%s' % (rid, pid))
        return dep

    def tx_begin(pid, is_readonly):
        return tracker.on_begin(pid)
        
    def tx_commit(pid):
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        yield from tracker.on_finish(pid, True)
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')

    def tx_abort(pid):
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='red')
        yield from tracker.on_finish(pid, False)
        
    def fini():
        print_general_stats(stats)
        print_failure_causes(stats)

    return NamedTuple(db_size=db_size,
                      tx_begin=make_generator(tx_begin),
                      tx_read=tx_read,
                      tx_write=tx_write,
                      tx_commit=tx_commit,
                      tx_abort=tx_abort,
                      fini=fini,
                      begin_tracking=tracker.begin_tracking,
                      end_tracking=tracker.end_tracking)


def make_seed(rng=random):
    '''Use the specified random number generator to create an 8-character
    base64 seed. If no rng is given, default to the global one.
    
    64**8 possible seeds (~ 10**15) should be enough...
    '''
    seed_material = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-'
    return ''.join(rng.choice(seed_material) for i in range(8))

def NumericRange(args):
    '''Specify a range of valid integer values that an RNG can return, in string format.

    One value specifies that the is a single number (no randomness)

    Two values (comma-separated) specify a min,max pair (order
    insensitive) from which values can be selected uniformly at
    random.

    '''
    class OneArg(object):
        def __init__(self, val):
            self.val = val
        def __call__(self, _):
            return self.val
        def __repr__(self):
            return repr(self.val)
        def range(self):
            return range(self.val,self.val+1)
        def scale(self, k):
            return OneArg(int(self.val*k))

    class TwoArg(object):
        def __init__(self, a, b):
            self.a,self.b = a,b
        def __call__(self, rng):
            return rng.randint(self.a, self.b)
        def __repr__(self):
            return '%r,%r' % (self.a,self.b)
        def range(self):
            return range(self.a,self.b+1)
        def scale(self, k):
            return TwoArg(int(self.a*k), int(self.b*k))
            
    args = tuple(map(int, args.split(',')))
    if len(args) == 1:
        return OneArg(*args)
    if len(args) == 2:
        return TwoArg(*sorted(args))

    raise TypeError('One or two arguments required')
            

def ratio_of_ranges(n, d):
    '''Compute the expected ratio of two NumericRange objects'''
    rn = n.range()
    en = sum(rn, 0.)/len(rn)
    rd = d.range()
    try:
        return len(rd)/sum(i/en for i in rd)
    except ZeroDivisionError:
        return 0


class Success(object): pass
class Failure(object): pass
class Total(object): pass

def canned_client(specfile, tx_timeout, duration, warmup, **extra_kwargs):
    '''A client used to replaying recorded runs from file. The format of a
    transaction is simple, and designed to work well with the
    multidelta tool (http://delta.tigris.org/). Each transaction
    consists of a list of semicolon-separated operations, wrapped in a
    set of curly braces. White space is only significant as a
    separator of tokens:

    {
        [class $CLASS_NAME;]
        begin 1.0;
        read 2.0 15;
        write 3.0 30;
        end 4.0;
    }

    "class" identifies the transaction's class, if a mixed workload is used
    
    "begin" identifies the start time of a transaction
    
    "read" and "write" identify the record to access and the time of the access

    "end" marks the end time of the transaction (the system decides
    whether it should commit or abort)

    '''
    def token_streamer(specfile):
        for line in open(specfile):
            yield from line.split()
        
    tx_specs = collections.defaultdict(list)
    cur_tx = None
    sentinel = object()
    tokens = token_streamer(specfile)
    tx_name = None
    def nextint():
        return int(next(tokens))
    def nextwhen():
        return float(next(tokens))
        
    while 1:
        token = next(tokens, sentinel)
        if token is sentinel:
            assert cur_tx is None
            break

        token = token.lower()
        need_semicolon = True
        if token == '{':
            assert cur_tx is None
            need_semicolon,cur_tx = False,[]

        elif token == 'class':
            tlist = []
            while 1:
                tok = next(tokens)
                if tok == ';':
                    tx_name = ' '.join(tlist)
                    need_semicolon = False
                    break
                else:
                    tlist.append(tok)
                    
        elif token == 'begin':
            assert not len(cur_tx)
            when = nextwhen()
            cur_tx.append((when,None,True))
            
        elif token == 'read':
            assert len(cur_tx)
            when,rid = nextwhen(),nextint()
            cur_tx.append((when,rid,True))

        elif token == 'write':
            assert len(cur_tx)
            when,rid = nextwhen(),nextint()
            cur_tx.append((when,rid,False))

        elif token == 'end':
            assert len(cur_tx)
            when = nextwhen()
            cur_tx.append((when,None,False))

        elif token == '}':
            assert len(cur_tx) > 1
            tx_specs[tx_name].append(cur_tx)
            need_semicolon,cur_tx,tx_name = False,None,None
            
        else:
            errlog('Unknown token: %s', token)
            exit(1)

        if need_semicolon:
            semicolon = next(tokens)
            assert semicolon == ';'
            
        
    # get some info from the benchmark harness...
    db, tracker, stats, n, seed, recorder, is_done = (yield)
    # ... then wait for the simulator to "start" the task
    yield
    
    def do_transaction(tx_name, tx_spec):
        pid = yield from sys_getpid()
        tracked = []
        try:
            yield from transaction(pid, tracked, tx_spec)
        except AbortTransaction:
            if tracked[0]:
                stats.failure_causes[tx_name,sys.exc_info()[0]] += 1
                stats.failure_causes[tx_name,Failure] += 1
            yield from db.tx_abort(pid)
        else:
            if tracked[0]:
                stats.failure_causes[tx_name,Success] += 1
                
        if tracked[0]:
            stats.failure_causes[tx_name,Total] += 1

    def transaction(pid, tracked, tx_spec):
        start = tx_spec[0][0]
        #errlog('Transaction %d started at %d', pid, when)
        started,ended = False,False
        read_only = not sum(int(not is_read)
                            for _,rid,is_read in tx_spec if rid is not None)
        for when,rid,is_read in tx_spec:
            #errlog('pid=%d when=%d rid=%s is_read=%s', pid, when, rid, is_read)
            if (when - start)//ONE_TICK > tx_timeout:
                raise TransactionTimeout
                
            yield from sys_sleep_until(int(when*ONE_TICK))
            if rid is None:
                is_begin = is_read
                if is_begin:
                    assert not started and not ended
                    yield from db.tx_begin(pid, read_only)
                    tracked.append(tracker.is_tracked(pid))
                    started = True
                else:
                    assert started and not ended
                    yield from db.tx_commit(pid)
                    ended = True
            else:
                assert started and not ended
                if is_read:
                    yield from db.tx_read(pid, rid)
                else:
                    yield from db.tx_write(pid, rid)

        assert started and ended

    for tx_name,specs in tx_specs.items():
        for spec in specs:
            yield from sys_spawn(do_transaction(tx_name, spec))

    # make sure it ends...
    end = warmup+duration+100
    end_spec = [ (end, None, True), (end+2, None, False) ]
    end_spec2 = [ (end+100, None, True), (end+102, None, False) ]
    yield from sys_spawn(do_transaction(None, end_spec))
    yield from sys_spawn(do_transaction(None, end_spec2))
    
    while 1:
        # sleep for some amount of time before submitting another task
        yield from sys_sleep(ONE_TICK*1000)

        # time to quit?
        if is_done():
            db.fini()
            yield from sys_exit()

def make_client(think_time, tx_name, tx_size, tx_writes,
                tx_timeout, max_inflight,
                **extra_kwargs):
    
    # get some info from the benchmark harness...
    db, tracker, stats, n, seed, recorder, is_done = (yield)
    # ... then wait for the simulator to "start" the task
    yield

    if recorder:
        out = recorder
        def recorder(spec):
            out('{')
            out('  class %s ;', tx_name)
            for when,rid,is_read in spec:
                when /= ONE_TICK
                if rid is None:
                    what = 'begin' if is_read else 'end'
                    out('  %s %s ;', what, when)
                else:
                    what = 'read' if is_read else 'write'
                    out('  %s %s %s ;', what,when,rid)
            out('}')
            out.flush()

        
    think_time = think_time.scale(ONE_TICK)
    cpid = yield from sys_getpid()

    # track completions separately: a newly-spawned task might
    # complete before the client adds it to the in-flight set.
    in_flight = set()
    completed = set()

    def do_transaction(nreads, accesses):
        pid = yield from sys_getpid()
        tracked,tx_spec = [],[]
        try:
            yield from transaction(pid, tx_spec, tracked, nreads, accesses)
        except AbortTransaction:
            if tracked[0]:
                stats.failure_causes[tx_name,Failure] += 1
                stats.failure_causes[tx_name,sys.exc_info()[0]] += 1
            if 0 and recorder:
                if tx_spec[-1][1] is not None or tx_spec[-1][2] is not False:
                    now = yield from sys_now()
                    tx_spec.append((now, None, False))
            yield from db.tx_abort(pid)
        else:
            if tracked[0]:
                stats.failure_causes[tx_name,Success] += 1

            recorder and recorder(tx_spec)
        
        if tracked[0]:
            stats.failure_causes[tx_name,Total] += 1
            
        # notify the client that we completed
        completed.add(pid)

    def transaction(pid, tx_spec, tracked, nreads, accesses):
        when = yield from sys_now()
        yield from sys_busy(ONE_TICK, color='yellow', title='client %d' % n)
        not recorder or tx_spec.append((when, None, True))
        read_only = nreads == len(accesses)
        yield from db.tx_begin(pid, read_only)
        tracked.append(tracker.is_tracked(pid))
        #errlog('Transaction %d started at %d', pid, when)
        for i,rid in enumerate(accesses):
            sleep = random.randint(ONE_TICK, 5*ONE_TICK)
            busy = random.randint(ONE_TICK, 2*ONE_TICK)
            yield from sys_sleep(sleep)
            now = yield from sys_now()
            if (now - when)//ONE_TICK > tx_timeout:
                raise TransactionTimeout

            is_read = i < nreads
            not recorder or tx_spec.append((now, rid, is_read))
            if is_read:
                yield from db.tx_read(pid, rid)
            else:
                yield from db.tx_write(pid, rid)
            yield from sys_busy(busy)

        now = yield from sys_now()
        not recorder or tx_spec.append((now, None, False))
        yield from db.tx_commit(pid)

    overloaded = False
    last_rec,write_ratio = db.db_size - 1, 0.25
    rng = random.Random(seed)
    #errlog('Client %d seed: %s', n, seed)
    while 1:
        # sleep for some amount of time before submitting another task
        yield from sys_sleep(think_time(rng))

        # time to quit?
        if is_done():
            when = yield from sys_now()
            errlog('Stragglers finished at %.2f', when/float(ONE_TICK))
            db.fini()
            yield from sys_exit()

        # for repeatability, always create the transaction
        nsteps = tx_size(rng)
        nwrites = tx_writes(rng)
        nreads = nsteps - nwrites
        accesses = [rng.randint(0, last_rec) for i in range(nsteps)]

        # process completions before declaring overload
        if len(completed) == max_inflight or len(in_flight) == max_inflight:
            in_flight -= completed
            completed.clear()

        # new tasks cannot spawn if overloaded
        if len(in_flight) < max_inflight:
            pid = yield from sys_spawn(do_transaction(nreads, accesses))
            in_flight.add(pid)
            overloaded = False
        elif 1 and not overloaded:
            overloaded = True
            now = yield from sys_now()
            errlog('t=%.2f: client %d overloaded (%s)',
                   now/float(ONE_TICK), n, ' '.join(map(str, sorted(in_flight))))


    
def make_benchmark(db_model, dep_tracker, clients, db_size, doffset,
                   warmup, duration, dscale, recorder, **extra_kwargs):
    stats = NamedTuple(failure_causes=collections.defaultdict(lambda: 0))
    tracker = dep_tracker(stats=stats, db_size=db_size, **extra_kwargs)
    db = db_model(db_size=db_size, tracker=tracker, stats=stats, **extra_kwargs)
    if dscale:
        duration //= (len(clients) - doffset)**dscale
    
    done = False
    def callback():
        nonlocal done
        done = True

    is_done = lambda: done
    
    for i,client in enumerate(clients):
        client.send(None)
        client.send((db, tracker, stats, i, make_seed(), recorder, is_done))
        yield from sys_spawn(client)

    when = yield from sys_now()
    errlog('Begin warmup period at %.2f', when/float(ONE_TICK))
    yield from sys_sleep(warmup*ONE_TICK)
    db.begin_tracking()
    when = yield from sys_now()
    errlog('Begin measurement at %.2f', when/float(ONE_TICK))
    yield from sys_sleep(duration*ONE_TICK)
    when = yield from sys_now()
    errlog('End measurement at %.2f', when/float(ONE_TICK))
    db.end_tracking(callback)
    
if __name__ == '__main__':
    test_tarjan()
    bench = make_benchmark(db_model=make_nocc_db, db_size=1000, nclients=100,
                           max_inflight=1000, warmup=200, duration=2000)
    simulator(bench, log=log_svg_task())
