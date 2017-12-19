import time
import redis
from redlock import RedLock

from celery import Celery
from celery import task

app = Celery('worker.tasks', backend='redis://localhost:6379/1', broker='redis://localhost:6379/0')

ADD_LOCK_KEY = "add_lock"
MULT_LOCK_KEY = "mult_lock"
MINUS_LOCK_KEY = "minus_lock"

ADD_REDIS_KEY_SUFFIX = ".tasks.add"
MULT_REDIS_KEY_SUFFIX = ".tasks.mult"
MINUS_REDIS_KEY_SUFFIX = ".tasks.minus"

@app.task(bind=True)
def add(self, x, y):
    """
    
    Executing add job with redlock
    
    Decorators:
        app
    
    Arguments:
        x {int} -- First param to be added
        y {int} -- Second param to be added
    
    Returns:
        int -- sum of the two params
    """
    
    current_worker_hostname = self.request.hostname
    tasks_in_current_worker = app.control.inspect().active()[current_worker_hostname]

    return execute_task_with_lock(_add, "add", ADD_LOCK_KEY, x, y)

def _add(x, y):
    """The acttual function doing add
    
    Sleep for 5 secs, and then add up the two params
    
    Arguments:
        x {int} -- First param to be added
        y {int} -- Second param to be added
    
    Returns:
        int -- sum of the two params
    """
    
    time.sleep(5)
    return x + y

@app.task(bind=True)
def mult(self, x, y):
    """
    
    Executing mult job with redlock
    
    Decorators:
        app
    
    Arguments:
        x {int} -- First param to be multed
        y {int} -- Second param to be multed
    
    Returns:
        int -- mult of the two params
    """
    current_worker_hostname = self.request.hostname
    tasks_in_current_worker = app.control.inspect().active()[current_worker_hostname]

    return execute_task_with_lock(_mult, "mult", MULT_LOCK_KEY, x, y)

def _mult(x, y):
    """The acttual function doing mult
    
    Sleep for 10 secs, and then multiply the two params
    
    Arguments:
        x {int} -- First param to be multed
        y {int} -- Second param to be multed
    
    Returns:
        int -- mult of the two params
    """
    time.sleep(10)
    return x * y

@app.task(bind=True)
def minus(self, x, y):
    """
    
    Executing minus job with redlock
    
    Decorators:
        app
    
    Arguments:
        x {int} -- First param to be minused from
        y {int} -- Second param to be used to minus
    
    Returns:
        int -- diff of the two params
    """
    current_worker_hostname = self.request.hostname
    tasks_in_current_worker = app.control.inspect().active()[current_worker_hostname]

    return execute_task_with_lock(_minus, "minus", MINUS_LOCK_KEY, x, y)

def _minus(x, y):
    """The acttual function doing minus
    
    Sleep for 15 secs, and then minus the two params
    
    Arguments:
        x {int} -- First param to be minus from
        y {int} -- Second param to be used to minus
    
    Returns:
        int -- difference of the two params
    """
    time.sleep(15)
    return x - y

def execute_task_with_lock(proc, task_name, lock_key, x, y):
    """Executing tasks with lock
    
    Executing the task, but only one at a time, meaning
    if there are the same tasks being processed, the incoming
    one will be stalled
    
    Arguments:
        proc {Function} -- Actual function performing the task
        task_name {string} -- Name of the task
        lock_key {string} -- Lock key for the task
        x {int} -- First argument of the task
        y {int} -- Second argument of the task
    
    Returns:
        int -- result of the task
    """
    
    result = -1

    has_no_lock = True

    while has_no_lock:
        try:
            with RedLock(lock_key):
                result = proc(x, y)
                has_no_lock = False
        except:
            print("waiting for the previous %s to complete, wait for 1 sec" % task_name)

            time.sleep(1)
            has_no_lock = True

    return result