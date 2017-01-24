import argparse
#import db_2pl, db_si, db_psi, db_ssi, db_nbl, db_ssi2, db_ssi3
import db_2pl, db_si, db_pg_ssi2, db_ssi3, db_ssi4, db_bli, db_nbl, db_2pl_ssn
from sim import errlog, simulator
from dbsim import *

models = dict(
    _nocc=make_nocc_db,
    _2pl=db_2pl.make_db,
    _si=db_si.make_db,
    #_psi=db_psi.make_psi_db,
    #_ssi=db_ssi.make_ssi_db,
    #_ssi2=db_ssi2.make_ssi2_db,
    _ssi3=db_ssi3.make_db,
    _ssi4=db_ssi4.make_db,
    _pg_ssi2=db_pg_ssi2.make_db,
    _bli=db_bli.make_db,
    _nbl=db_nbl.make_db,
    _2pl_ssn=db_2pl_ssn.make_db,
)

class Model(object):
    def __init__(self, name):
        self.name = name
        self.make_it = models['_'+name.lower()]
            
    def __str__(self):
        return self.name
    def __call__(self, *args, **kwargs):
        return self.make_it(*args, **kwargs)

trackers = dict(_safe=safe_dependency_tracker,
                #_precise=precise_dependency_tracker,
            )

class Tracker(object):
    def __init__(self, name):
        self.name = name
        self.make_it = trackers['_'+name.lower()]
            
    def __str__(self):
        return self.name
    def __call__(self, *args, **kwargs):
        return self.make_it(*args, **kwargs)

class Log(object):
    def __init__(self, name):
        self.name = name
        name = name.lower()
        if name == 'none':
            self.make_it = log_devnull
        elif name == 'svg_task':
            self.make_it = log_svg_task
        else:
            raise KeyError('Unrecognized log consumer: %s')
            
    def __str__(self):
        return self.name
    def __call__(self, *args, **kwargs):
        return self.make_it(*args, **kwargs)
    
str2set = lambda x: set(int(y) for y in x.split(','))
def make_bool(x):
    x = x.lower()
    if x in ('', 'false', '0', 'no'):
        return False
    return True

class client_factory_base(argparse.Action):
    '''Base class for client factories'''
    
    class ClientList(list):
        def __init__(self):
            self.nbatches = 0
        def __repr__(self):
            return '%d <in %d batch(es)>' % (len(self), self.nbatches)
            
    def __call__(self, parser, namespace, option_value, option_string):
        clients = getattr(namespace, self.dest, None) or self.ClientList()
        clients.nbatches += 1
        had_name = bool(namespace.tx_name)
        if not had_name:
            namespace.tx_name = 'Client class %d' % clients.nbatches

        self.make_clients(vars(namespace), clients, option_value)
        if not had_name:
            namespace.tx_name = None
        setattr(namespace, self.dest, clients)
        
class client_factory(client_factory_base):
    '''Create the specified number of clients, using values accumulated so
    far from the namespace

    '''
    def make_clients(self, args, clients, nclients):
        errlog('Configuration for %d clients:', nclients)
        for k in 'tx_name tx_size tx_writes think_time tx_timeout max_inflight'.split():
            errlog('\t%-16s %s', k, args[k])
        errlog('')
        
        clients.extend(make_client(**args) for i in range(nclients))
        
class replay_factory(client_factory_base):
    '''Replay a set of transactions from file as if from a client set,
    assigning them a name in the usual way.

    '''
    def make_clients(self, args, clients, specfile):
        errlog('Replaying file %s', specfile)
        clients.append(canned_client(specfile=specfile, **args))

class Recorder(object):
    def __init__(self, fname):
        #errlog('Logging all transactions to %s', fname)
        self.fname,self.ofile = fname,open(fname,'w')
        self.out = make_log(self.ofile)
    def __call__(self, *args):
        #errlog('calling recorder%s', args)
        self.out(*args)
    def flush(self):
        self.ofile.flush()
    def __repr__(self):
        return '<recording transactions in %s>' % self.fname
        
parser = argparse.ArgumentParser(description='Database workload simulator')
x = parser.add_argument
x('--run-name',
  help='Give a name to this run (for easier parsing multi-run output logs)')
x('--db-model', type=Model, required=True,
  help='Database model to use (available models: 2PL, SI)')
x('--dep-tracker', type=Tracker, required=True,
  help='''Dependency tracker to use.

  The "safe" tracker, used by Ports and Grittner's SSI, checks for
  serialization failures and discards finalized transactions in
  batches whenever a new safe point is installed (a safe point is the
  commit time of the latest transaction known not to have any
  depencies on any in-flight transaction).

  The "precise" tracker, developed by me, checks for serialization
  failures more frequently and discards finalized transactions as soon
  as their dependencies have all been finalized.''')
x('--verbose', type=make_bool, default=False,
  help='If true, dump a lot of debug info to stderr')
x('--allow-cycles', type=make_bool, default=False,
  help='If true, dependency trackers will detect and record cycles; otherwise, cycles cause the simulation to abort') 
x('--seed', type=str, default=None,
  help='The RNG seed to use')
x('--db-size', type=int, default=1000,
  help='Number of records in the database')
x('--nrec', dest='db_size', type=int, default=1000,
  help='(deprecated) Alias for --db-size')
x('--replay-file', dest='clients', type=str, required=False, action=replay_factory,
  help='Replay a set of transactions from file')
x('--nclients', dest='clients', type=int, required=False, action=client_factory, 
  help='Number of clients submitting requests')
x('--max-inflight', type=int, default=1000,
  help='Number of in-flight transactions a client is willing to tolerate')
x('--warmup', type=int, default=1000,
  help='Number of ticks to warm up before starting the measurement')
x('--duration', type=int, default=2000,
  help='Number of ticks in the measurement window')
x('--dscale', type=float, default=0,
  help='If non-zero, scale the duration of each run by nclients**-dscale (so that more clients gives shorter runs)')
x('--doffset', type=int, default=0,
  help='Ignore the first doffset clients when applying dscale')
x('--log', dest='log_type', default=None, type=Log,
  help='Log consumer to use (available: none, svg_task)')
x('--report-cycles', type=int, default=0,
  help='''If > 0, report TID of transactions involved in serial dependency cycles.
  If > 1, draw swimlane charts and emit dependency graphs as well.''')

x('--record-file', dest='recorder', type=Recorder, default=None,
  help='If true, record a replayable trace of the workload in the specified file')
x('--tx-name', type=str, default=None,
  help='Name this client class, useful when more than one client class runs')
x('--tx-size', type=NumericRange, default=NumericRange('8,12'),
  help='Number of accesses each transaction makes')
x('--tx-writes', type=NumericRange, default=NumericRange('1,4'),
  help='Number of accesses each transaction makes')
x('--think-time', type=NumericRange, default=NumericRange('50,150'),
  help='Client think time')
x('--tx-timeout', type=int, default=2000,
  help='Kill any transaction that takes longer than this to finish')
x('--admission-limit', type=int,
  help='Limit the system to a fixed number of in-flight transactions (rejecting extra requests)')

x('--si-relax-writes', type=make_bool, default=False, help='Allow blind writes that would normally fall outside the snapshot. This can lead to undetected cycles under traditional SSI, but proof-guided SSI handles it just fine.')
x('--si-relax-reads', type=int, default=0, help='If 1, read a version committed after the snapshot only if accessing the overwritten one would force an abort. If 2, discard the snapshot entirely, always reading hte latest version. Either setting can lead to undetected cycles under traditional SSI, but proof-guided SSI handles it just fine.')

# specialized parameters
x('--limit-wait-depth', type=int, default=1,
  help='In 2PL, abort any transaction that would wait on a lock whose holder is already waiting')
x('--tid-watch', type=str2set, default=(),
  help='In PSI, trace all accesses made by tids on this list')
x('--rid-watch', type=str2set, default=(),
  help='In PSI, trace all accesses to rids on this list')
x('--analyze-tids', type=str2set, default=(),
  help='Analyze the neighborhood of dependencies involving tids on this list')
x('--danger-check', type=int, default=2,
  help='In PSI, check dependencies for Dangerous Structures (1), Dangerous Structures and Skew (2), or run unprotected (0). Anything other than 2 risks wedging the database.')
x('--ww-blocks', type=make_bool, default=True,
  help='In PSI, block on uncommitted WW conflict')
x('--wr-blocks', type=make_bool, default=False,
  help='In PSI, block on uncommitted WR conflict')
x('--read-only-opt', type=make_bool, default=True,
  help='In SSI and similar, treat read-only transactions as committing at their snapshot time')
x('--safe-snaps', type=int, default=0,
  help='In SSI and similar, a transaction known a priori to be read-only will execute under SI using a "safe" snapshot that is guaranteed to be anomaly-free. The value (which must be positive) is the minimum time separating safe snapshots.')
x('--version-reader', type=make_bool, default=False,
  help='In SSN, allow each version to remember one in-flight read (so the reader need not revisit it during pre-commit)')

args = parser.parse_args()
if not args.clients:
    errlog('No workload specified! (did you forget --nclients or --replay-file?)')
    exit(1)

# add auxiliary info (computed from other args)
args._tx_write_ratio = ratio_of_ranges(args.tx_writes, args.tx_size)

if not args.seed:
    args.seed = make_seed()

random.seed(args.seed)

args = vars(args)
errlog('Simulator configuration:')
for k,v in sorted(args.items()):
    errlog('\t%-16s %s', k, v)
errlog('')

del args['seed']

simulator(make_benchmark(**args), **args)
