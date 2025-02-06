from functools import partial, wraps
from operator import attrgetter
from typing import Any, Callable, List, Optional

from ..app_settings import app_settings
from ..constants import TASK_EXTRA_CONTEXT_ATTRIBUTE
from ..scheduler import BaseSchedulerBackend
from ..types import SchedulerJob
from ..utils import are_subscriptions_valid
from .commands import (
    AddJobCommand,
    CommandQueue,
    DeleteJobCommand,
    UpdateJobCommand,
)
from .registry import task_registry


def register_task(
    subscriptions: List[str] = [],
    name: Optional[str] = None,
    schedule: Optional[SchedulerJob] = None,
    **kwargs,
) -> Callable[[Callable], Callable]:
    def decorator(f: Callable) -> Callable:
        from ..publisher import send_task

        task_name = name or f.__name__

        @wraps(f)
        def callback(*args: Any, **kwargs: Any) -> Any:
            return f(*args, **kwargs)

        callback.delay = partial(send_task, task_name=task_name)
        callback.subscriptions = list(map(attrgetter("value"), subscriptions))
        callback.name = task_name

        if are_subscriptions_valid(subscriptions):
            task_registry.register(callback, name=task_name, raw_schedule=schedule)
            if schedule:
                CommandQueue.append(AddJobCommand(task_name))

        setattr(callback, TASK_EXTRA_CONTEXT_ATTRIBUTE, kwargs)

        return callback

    return decorator


def sync_scheduler():
    scheduler_service: BaseSchedulerBackend = app_settings.SCHEDULER_BACKEND_CLASS()

    for job in (
        cloud_scheduler_jobs := getattr(scheduler_service.list_jobs(), "jobs", [])
    ):
        if (
            job.name not in task_registry.tasks
            or job.name not in task_registry.schedule_configs
        ):
            CommandQueue.append(DeleteJobCommand(job.name))
            continue

        if job.name in task_registry.schedule_configs:
            schedule_config = task_registry.schedule_configs[job.name]
            if any(
                job.schedule != schedule_config.schedule,
                job.time_zone != schedule_config.time_zone,
                job.pubsub_target != schedule_config.pubsub_target,
            ):
                CommandQueue.append(UpdateJobCommand(job.name))
            continue

    for task_name in task_registry.schedule_configs:
        if task_name not in cloud_scheduler_jobs:
            CommandQueue.append(AddJobCommand(task_name))

    CommandQueue.execute_commands()
