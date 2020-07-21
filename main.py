#!/usr/bin/env python3

import logging
import os
import time
import datetime
import abc
import shutil
import pyinotify

import handlers
import scheduler


logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger("media_archiver")

SCHEDULER_TICK_TIME = 5
FULL_SCAN_TIME = 1 * 60 * 60 # Every Hour

OUT_DIR = None
PASSIVE_WATCH_DIRS = None
MANAGED_WATCH_DIRS = None

SCHEDULER = scheduler.Scheduler()


def parse_watch_dirs(env_name):
    dirs = os.getenv(env_name, None)
    if not dirs:
        return []

    dirs = dirs.split(",")

    for d in dirs:
        if not os.path.isdir(d):
            raise Exception("{} is not a valid directory".format(d))

    return dirs


def run_entry(file_path, out_dir):
    """
    Return True if we need to reschedule, False if not
    """
    delete = should_delete(file_path)
    handler = handlers.handler_factory(file_path, out_dir, delete)

    if handler is None:
        return True

    return handler.run()
    

def schedule_entry(file_path, out_dir):
    delete = should_delete(file_path)
    handler = handlers.handler_factory(file_path, out_dir, delete)

    if handler is None:
        return

    if handler.delay != 0:
        SCHEDULER.add_delayed_task(run_entry, args=(file_path, out_dir), delay=handler.delay)
    else:
        SCHEDULER.add_task(run_entry, args=(file_path, out_dir))


def should_delete(file_path):
    global MANAGED_WATCH_DIRS
    global PASSIVE_WATCH_DIRS

    # if the directory is managed by us, delete the file after
    # transfer
    for d in MANAGED_WATCH_DIRS:
        if os.path.commonprefix((d, file_path)) == d:
            return True
    
    # We do not touch anything with the passive watch dirs.
    for d in PASSIVE_WATCH_DIRS:
        if os.path.commonprefix((d, file_path)) == d:
            return False
    
    return False

    
class inotify_trigger(pyinotify.ProcessEvent):
    def process_default(self, event):
        global OUT_DIR
        dir = event.path
        file = event.name
        new_entry = os.path.join(dir, file)
        schedule_entry(new_entry, OUT_DIR)


def rec_add_watches(path, wm):
    for root, dirs, files in os.walk(path):

        # process directories
        for d in dirs:
            curr_dir = os.path.join(root, d)
            rec_add_watches(curr_dir, wm)

        logger.info("Adding {} to watchlist".format(path))
        wm.add_watch(path, pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO, rec=True, auto_add=True, proc_fun=inotify_trigger())


def walk_dir(dir, out_dir):
    for root, dirs, files in os.walk(dir):

        # process directories
        for d in dirs:
            curr_dir = os.path.join(root, d)
            schedule_entry(curr_dir, out_dir)
            walk_dir(curr_dir, out_dir)

        # process files in this directory
        for f in files:
            schedule_entry(os.path.join(root, f), out_dir)

def full_scan():
    for path in PASSIVE_WATCH_DIRS + MANAGED_WATCH_DIRS:
        walk_dir(path, OUT_DIR)


def main():
    global MANAGED_WATCH_DIRS
    global PASSIVE_WATCH_DIRS
    global OUT_DIR

    # managed watch dirs, once a file has been processed, it will be deleted
    MANAGED_WATCH_DIRS = parse_watch_dirs('MANAGED_WATCH_DIRS')
    
    # passive watch dirs, once a file has been processed, it will be left in place.
    PASSIVE_WATCH_DIRS = parse_watch_dirs('PASSIVE_WATCH_DIRS')
      
    if len(MANAGED_WATCH_DIRS) == 0 and len(PASSIVE_WATCH_DIRS) == 0:
        raise Exception("MANAGED_WATCH_DIRS and/or PASSIVE_WATCH_DIRS must be set")
    
     # directory to archive the file
    out_dir = parse_watch_dirs('OUT_DIR')
    if len(out_dir) != 1:
        raise Exception("OUT_DIR not set correctly: {}".format(out_dir))
    OUT_DIR = out_dir[0]

    logger.info("Watching the following managed directories: {}".format(MANAGED_WATCH_DIRS))
    logger.info("Watching the following passive directories: {}".format(PASSIVE_WATCH_DIRS))
    logger.info("Archiving files to : {}".format(OUT_DIR))
    
    wm = pyinotify.WatchManager()
    notifier = pyinotify.ThreadedNotifier(wm)

    for path in PASSIVE_WATCH_DIRS + MANAGED_WATCH_DIRS:
        rec_add_watches(path, wm)   
             
    notifier.start()

    full_scan()

    try:
        while True:
            SCHEDULER.run()
            time.sleep(SCHEDULER_TICK_TIME)
    except KeyboardInterrupt:
        pass
    finally:
        notifier.stop()

if __name__ == "__main__":
    main()