import time
import threading
from queue import PriorityQueue

class Task:
    def __init__(self, func, id):
        self.id = id
        self.func = func
        self.time = time.time()

# should have a delayed pq to store all incoming tasks
class Scheduler(threading.Thread):
    def __init__(self, max_size):
        self.max_size = max_size
        self.lock = threading.Lock()
        self.taskQ = PriorityQueue()
        self.not_empty_cv = threading.Condition(self.lock)
        self.not_full_cv = threading.Condition(self.lock)
        self.ready_cv = threading.Condition(self.lock)

    # sort each task by their start_time (time+delay)
    def add_task(self, task, delay):
        if delay < 0:
            raise Exception("delay cannot be negative")

        start_time = delay + task.time
        with self.not_full_cv:
            if self.taskQ.qsize() >= self.max_size:
                raise Exception("full queue")
            # can still add in new task
            self.taskQ.put([start_time, task])
            self.not_empty_cv.notify()
            if delay == 0:
                self.ready_cv.notify()

    def execute_task(self):
        with self.ready_cv:
            if not self.taskQ.qsize():
                raise Exception("task queue is empty - no task to run")
            curr_time = time.time()
            start_time = self.peek_queue()[0]
            if curr_time < start_time: 
                raise Exception("not execute time yet")
            self.not_full_cv.notify()
            run_task = self.taskQ.get()[1]
        return run_task

    def peek_queue(self):
        return self.taskQ.queue[0]

		# entry point of thread
		def run(self):
				pass


# main
def task_func(id):
    print("func ", id)

tasks = []
for i in range(1, 4):
    task = Task(task_func, i)
    tasks.append(task)

def exe():
    try:
        task = s.execute_task()
    except Exception as e:
        print(e)
    else:
        print("running task", task.id)

s = Scheduler(10)
s.start()
for task in tasks:
    print("adding task", task.id)
    s.add_task(task, 2)

time.sleep(2)
exe()
time.sleep(2)
exe()
time.sleep(2)
exe()
time.sleep(2)
exe()
