import collections, random, sys, weakref
from dbsim import *
    
class WriteSkew(AbortTransaction):
    '''Overwrote a version created after my snapshot'''
    
def make_db(tracker, stats, db_size, **extra_kwargs):
    resp_histo = collections.defaultdict(lambda:0)
    in_flight = {}

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

    def tx_read(pid, rid, for_update=False):
        def read_filter(it):
            # take the newest version that precedes our snapshot
            start = tracker.get_begin(pid)
            for x,dep,stamp in it:
                if pid == dep or stamp < start:
                    return x

            errlog('Oops! T=%d finds no suitable version of R=%d', pid, rid)
            errlog('\t(last version was %d, visible since %s, T started at %d)',
                   dep, stamp, start)
            assert not 'reachable'
                    
        val,_ = yield from tracker.on_access(pid, rid, read_filter)
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='green', title='%s=db[%s]' % (val, rid))
        return val

    def tx_write(pid, rid):
        dep,_ = yield from tracker.on_access(pid, rid, False)
        if dep != pid and tracker.is_known(dep):
            commit = tracker.is_committed(dep)
            assert commit
            if tracker.get_begin(pid) < commit:
                raise WriteSkew
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK),
                            color='blue', title='db[%s]=%s' % (rid, pid))

    def tx_begin(pid, is_readonly):
        in_flight[pid] = yield from sys_now()
        tracker.on_begin(pid)
        
    def finish(pid, is_commit):
        yield from tracker.on_finish(pid, is_commit)
        then,now = in_flight.pop(pid), (yield from sys_now())
        histo_add(resp_histo, then-now)

    def tx_commit(pid):
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='yellow')
        yield from finish(pid, True)
        yield from sys_sleep(random.randint(5*ONE_TICK, 10*ONE_TICK))
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='orange')
        
    def tx_abort(pid):
        # log rollback is not free...
        yield from sys_busy(random.randint(ONE_TICK, 2*ONE_TICK), color='red')
        yield from finish(pid, False)


    def fini():
        if in_flight:
            errlog('\nFound %d live transactions at exit (oldest from tick %.2f)',
                   len(in_flight), min(in_flight.values())/float(ONE_TICK))

        print_general_stats(stats)
        print_failure_causes(stats)
        histo_print(resp_histo, 'transaction response times')


    return NamedTuple(db_size=db_size, tx_begin=tx_begin,
                      tx_read=tx_read, tx_write=tx_write,
                      tx_commit=tx_commit, tx_abort=tx_abort,
                      fini=fini,
                      begin_tracking=tracker.begin_tracking,
                      end_tracking=tracker.end_tracking)
        
                


    
if __name__ == '__main__':
    try:
        seed = sys.argv[1]
    except:
        seed = make_seed()

    errlog('Using RNG seed: %s', seed)
    random.seed(seed)

    simulator(make_benchmark(make_db=make_si_db, nclients=50, max_inflight=100, db_size=1000, duration=10000), log=log_svg_task())
