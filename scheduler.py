import time
import threading
import logging

logger = logging.getLogger("media_archiver")

class Scheduler():

    def __init__(self):
        self._tasks = []
        self._lock = threading.Lock()

    def _add_delayed_task(self, task, args, delay):
        self._tasks.append({"task": task, "args": args, "scheduled": time.time() + delay, "delay": delay})
    
    def add_delayed_task(self, task, args, delay):
        self._lock.acquire()
        self._add_delayed_task(task, args, delay)
        self._lock.release()
    
    def add_task(self, task, args):
        self._lock.acquire()
        self._add_delayed_task(task, args, 0)
        self._lock.release()

    def run(self):
        self._lock.acquire()
            
        for t in self._tasks:
            if t["scheduled"] < time.time():
                logger.debug("Running task with args: {}, delay: {}".format(t["args"], t["delay"]))
                success = t["task"](*t["args"])
                self._tasks.remove(t)

                if not success:
                    logger.info("Rescheduling task with args: {}, delay: {}".format(t["args"], t["delay"]))
                    self._add_delayed_task(t["task"], t["args"], t["delay"])
        self._lock.release()
