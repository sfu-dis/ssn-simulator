'''A discrete event simulator kernel based on coroutines. The
simulator exposes a set of "system calls" that allow tasks to start
and communicate with each other.

Inspired by http://www.dabeaz.com/coroutines/Coroutines.pdf

'''
import collections, heapq, itertools, sys

# simulators use timestamps with a resolution of ONE_TICK subticks per
# "tick"
ONE_TICK = 1000

def make_log(out):
    def thunk(msg, *args):
        if args:
            msg %= args
        out.write('%s\n' % msg)

    return thunk

errlog = make_log(sys.stderr)

def log_devnull():
    while 1:
        yield

# SVGPan is BSD-licensed code downloaded from
# http://www.cyberz.org/projects/SVGPan/SVGPan.js
class svg_printer(object):
    '''Helper class for converting simulator output log into an SVG image

    To make large images more navigable, it uses SVGPan, a
    BSD-licensed pan and zoom code downloaded from
    http://www.cyberz.org/projects/SVGPan/SVGPan.js (a local copy of
    which is assumed to be in the current directory).

    '''
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
    '''A log consumer that turns simulator events into an SVG image
    showing per-task timelines.

    '''
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

# syscall numbers (no need to use these)
_sys_cpu = 0
_sys_now = 1
_sys_sleep = 2
_sys_busy = 3
_sys_getpid = 4
_sys_park = 5
_sys_unpark = 6
_sys_spawn = 7
_sys_exit = 8
_sys_sleep_until = 9

# simulator syscalls
def sys_cpu():
    '''Return the CPU the task currently runs on'''
    return (yield (_sys_cpu,))
def sys_now():
    '''Return the current simulation timestamp'''
    return (yield (_sys_now,))
def sys_sleep(delay):
    '''Deschedule the current task for at least "delay" subticks'''
    yield _sys_sleep, delay
def sys_sleep_until(when):
    '''Reschedule the current task to resume no earlier than the timestamp given'''
    yield _sys_sleep_until, when
def sys_busy(delay, **kwargs):
    '''Notify the simulator that the task has worked for "delay" subticks
    
    Optional kwargs can be used to give more information about what
    work was done, usually in cooperation with a custom log consumer.

    The log_svg_task consumer recognizes "title" and "color"
    attributes, for example, which respectively become the tooltip
    text and background color of a time chunk in the SVG timeline.

    '''
    yield _sys_busy, delay, kwargs
def sys_getpid():
    '''Return the PID of the current task'''
    return (yield (_sys_getpid,))
def sys_park():
    '''Deschedule and forget the current task.

    The task will not run again unless some other task reschedules
    it. Useful for implementing "user-space" synchronization
    primitives.

    '''
    return (yield (_sys_park,))
def sys_unpark(pid):
    '''Make the simulator aware of a previously-parked task, allowing it
    to run again immediately

    '''
    yield _sys_unpark, pid
def sys_spawn(task):
    '''Create a new task and return its PID

    WARNING: the caller must assume that the newly-created task might
    complete before this call returns, unless additional steps are
    taken to force the new task to wait for its creator to become
    aware of it.

    '''
    return (yield _sys_spawn, task)
def sys_exit(rval=0):
    '''Immediately terminate the simulation with the given return code.

    This call does not return, and no other task will execute any more
    code.

    '''
    yield _sys_exit, rval

def simulator(init_task, log=None, **extra_kwargs):
    '''A discrete event-based simulator allowing any degree of
    concurrency. All tasks run exactly when scheduled.

    Simulator state is initialized with a single "seed" task that will
    be scheduled as if by a call to sys_spawn() at time zero. The
    simulator's execution is completely deterministic, and syscalls
    are "free" in simulated time. Scheduling-related syscalls (e.g.
    sys_busy, sys_sleep, sys_park) have no timing-related side effects
    beyond those requested by the user.

    '''
    # simulator loop variables
    task, pid, fn, args, delay, reschedule = (None,)*6
    
    # initialize the log
    log = log or log_devnull()
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
    def sys_sleep_until(when):
        if when > now:
            sys_sleep(when - now)
        
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
    syscalls[_sys_sleep_until] = sys_sleep_until

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
        except KeyboardInterrupt:
            errlog('interrupted while waiting for %s', task)
            task.throw(*sys.exc_info())
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
        errlog('Simulator: %d parked tasks at exit (oldest parked at tick %.2f)',
               len(parked_tasks), min(then for _,then in parked_tasks.values())/float(ONE_TICK))
        #for pid,(task,then) in parked_tasks.items():
        #    errlog('\tpid=%d since %.2f (%d ticks ago)'
        #           % (pid, then/float(ONE_TICK), (now-then)//ONE_TICK))
        
    return rval
