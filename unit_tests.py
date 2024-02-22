import pytest
from task_queue import Resources, Task, TaskQueue


@pytest.fixture
def task_queue():
    return TaskQueue()


@pytest.fixture
def resources0():
    return Resources(ram=0, cpu_cores=1, gpu_count=0)


@pytest.fixture
def resources():
    return Resources(ram=512, cpu_cores=1, gpu_count=0)


@pytest.fixture
def resources2():
    return Resources(ram=1024, cpu_cores=2, gpu_count=0)


@pytest.fixture
def task(resources):
    return Task(id=1, priority=100, resources=resources, content='content')


@pytest.fixture
def task2(resources2):
    return Task(id=2, priority=10, resources=resources2, content='content')


def test_simple_add(task_queue, task):
    task_queue.add_task(task)
    assert task_queue.size() == 1


def test_simple_pop(task_queue, resources, task):
    task_queue.add_task(task)
    assert task_queue.get_task(available_resources=resources) == task
    assert task_queue.size() == 0


def test_resources_constraint(task_queue, task, resources0):
    task_queue.add_task(task)
    task_queue.get_task(available_resources=resources0)
    assert task_queue.size() == 1


def test_priority_order(task_queue, task, task2, resources2):
    task_queue.add_task(task)
    task_queue.add_task(task2)
    t = task_queue.get_task(available_resources=resources2)
    assert t.priority == min(task.priority, task2.priority)


def test_queue_remembering_tasks(task_queue, task, task2, resources):
    task_queue.add_task(task)
    task_queue.add_task(task2)
    t = task_queue.get_task(available_resources=resources)
    assert task_queue.size() == 1
