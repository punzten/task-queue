"""
Task description:
* Requires a task queue with priorities and resource limits.
* Each task has a priority and the required amount of resources to process it.
* Publishers create tasks with specified resource limits, and put them in a task queue.
* Consumer receives the highest priority task that satisfies available resources.
* The queue is expected to contain thousands of tasks.
* Write a unit test to demonstrate the operation of the queue.
"""

from dataclasses import dataclass, fields
from queue import PriorityQueue
from functools import total_ordering


@dataclass()
class Resources:
    ram: int
    cpu_cores: int
    gpu_count: int


@total_ordering
@dataclass(eq=False)
class Task:
    id: int
    priority: int
    resources: Resources
    content: str
    result: str = ""

    def __eq__(self, other):
        return self.priority == other.priority

    def __lt__(self, other):
        return self.priority < other.priority


class TaskQueue:
    def __init__(self):
        self.queue = PriorityQueue()

    def size(self) -> int:
        return self.queue._qsize()

    def add_task(self, task: Task):
        self.queue.put(task)

    def get_task(self, available_resources: Resources) -> Task:
        not_satisfied = []

        while not self.queue.empty():
            task = self.queue.get()

            if all(
                    [getattr(available_resources, field.name) >= getattr(task.resources, field.name)
                     for field in fields(Resources)]
            ):
                for task in not_satisfied:
                    self.queue.put(task)
                return task
            else:
                not_satisfied.append(task)

        for task in not_satisfied:
            self.queue.put(task)

        return None