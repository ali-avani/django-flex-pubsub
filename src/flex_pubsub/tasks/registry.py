from typing import Callable, Dict, Optional

from google.cloud.scheduler_v1.types.job import Job

from ..app_settings import app_settings
from ..scheduler import BaseSchedulerBackend
from ..types import SchedulerJob


class TaskRegistry:
    def __init__(self) -> None:
        self.tasks: Dict[str, Callable] = {}
        self.schedule_configs: Dict[str, Job] = {}

    def register(
        self,
        func: Optional[Callable] = None,
        *,
        name: Optional[str] = None,
        raw_schedule: Optional[SchedulerJob] = None,
    ) -> Callable:
        if func is None:
            return lambda f: self.register(f, name=name, raw_schedule=raw_schedule)
        task_name = name or func.__name__
        self.tasks[task_name] = func
        if raw_schedule and (schedule := SchedulerJob.model_validate(raw_schedule)):
            self.schedule_configs[task_name] = schedule.model_dump()
            scheduler_backend: BaseSchedulerBackend = (
                app_settings.SCHEDULER_BACKEND_CLASS()
            )
            scheduler_backend.schedule(task_name, schedule)
        return func

    def _get_job_name(self, job: Job):
        return job.name.split("/")[-1]

    def get_task(self, name: str) -> Optional[Callable]:
        return self.tasks.get(name)

    def get_schedule_config(self, name: str) -> Optional[Job]:
        return self.schedule_configs.get(name)

    def get_all_tasks(self) -> Dict[str, Callable]:
        return self.tasks


task_registry = TaskRegistry()
