from abc import ABC, abstractmethod
from collections import deque
from typing import ClassVar, Literal

from ..app_settings import app_settings
from ..events import commands_post_execute, commands_pre_execute
from ..scheduler import BaseSchedulerBackend
from .registry import task_registry


class BaseCommand(ABC):
    action: ClassVar[Literal["ADD", "UPDATE", "DELETE"]]

    def __init__(self, task_name: str):
        self.task_name = task_name
        self.scheduler_service: BaseSchedulerBackend = (
            app_settings.SCHEDULER_BACKEND_CLASS()
        )

    @abstractmethod
    def execute(self):
        pass


class AddJobCommand(BaseCommand):
    action = "ADD"

    def execute(self):
        if not (scheduler := task_registry.get_schedule_config(self.task_name)):
            return

        return self.scheduler_service.create_job(self.task_name, scheduler)


class UpdateJobCommand(BaseCommand):
    action = "UPDATE"

    def execute(self):
        if not (scheduler := task_registry.get_schedule_config(self.task_name)):
            return

        return self.scheduler_service.update_job(self.task_name, scheduler)


class DeleteJobCommand(BaseCommand):
    action = "DELETE"

    def execute(self):
        return self.scheduler_service.delete_job(self.task_name)


class CommandQueue:
    _queue: deque[BaseCommand] = deque()

    @classmethod
    def append(cls, command):
        cls._queue.append(command)

    @classmethod
    def set_queue(cls, queue: deque):
        cls._queue = queue

    @classmethod
    def pop(cls):
        return cls._queue.popleft()

    @classmethod
    def inspect(cls):
        return list(cls._queue.copy())

    @classmethod
    def execute_commands(self):
        commands_pre_execute.send(sender=self)
        while self._queue:
            command = self.pop()
            command.execute()
        commands_post_execute.send(sender=self)
