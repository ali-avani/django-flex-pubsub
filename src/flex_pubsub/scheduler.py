import contextlib
import json
import logging

try:
    from google.api_core.exceptions import AlreadyExists, NotFound
    from google.cloud import scheduler_v1
    from google.cloud.scheduler_v1.types.cloudscheduler import (
        CreateJobRequest,
        DeleteJobRequest,
        ListJobsRequest,
        ListJobsResponse,
        UpdateJobRequest,
    )
    from google.cloud.scheduler_v1.types.job import Job
    from google.cloud.scheduler_v1.types.target import PubsubTarget
except ImportError:
    scheduler_v1 = None

from .app_settings import app_settings
from .types import SchedulerJob

logger = logging.getLogger("flex_pubsub")


class BaseSchedulerBackend:
    def delete_job(self, task_name: str, scheduler: SchedulerJob) -> None:
        raise NotImplementedError

    def create_job(self, task_name: str, scheduler: SchedulerJob) -> None:
        raise NotImplementedError

    def update_job(self, task_name: str, scheduler: SchedulerJob) -> None:
        raise NotImplementedError

    def list_jobs(self) -> ListJobsResponse:
        raise NotImplementedError

    def schedule(self, task_name: str, schedule_config: SchedulerJob) -> Job:
        raise NotImplementedError


class LocalSchedulerBackend(BaseSchedulerBackend):
    def delete_job(self, task_name: str) -> None:
        logger.info(
            f"LocalSchedulerBackend: delete_job called with task_name={task_name}"
        )

    def create_job(self, task_name: str, schedule_config: SchedulerJob) -> Job:
        logger.info(
            f"LocalSchedulerBackend: create_job called with task_name={task_name}, schedule_config={schedule_config}"
        )
        return Job(name=task_name)

    def update_job(self, task_name: str, schedule_config: SchedulerJob) -> Job:
        logger.info(
            f"LocalSchedulerBackend: update_job called with task_name={task_name}, schedule_config={schedule_config}"
        )
        return Job(name=task_name)

    def list_jobs(self) -> ListJobsResponse:
        logger.info("LocalSchedulerBackend: list_jobs called")
        return ListJobsResponse()

    def schedule(self, task_name: str, schedule_config: SchedulerJob) -> Job:
        logger.info(
            f"LocalSchedulerBackend: schedule called with task_name={task_name}, schedule_config={schedule_config}"
        )
        return Job(name=task_name)


class GoogleSchedulerBackend(BaseSchedulerBackend):
    def __init__(self) -> None:
        if scheduler_v1 is None:
            raise ImportError("google-cloud-scheduler is not installed.")
        credentials = app_settings.GOOGLE_CREDENTIALS
        self.client = scheduler_v1.CloudSchedulerClient(credentials=credentials)
        self.project_id = app_settings.GOOGLE_PROJECT_ID
        self.location = app_settings.SCHEDULER_LOCATION

        self.parent = self._get_parent()
        logger.info("Initialized GoogleSchedulerBackend")

    def job_from_scheduler(self, scheduler: SchedulerJob):
        return Job(
            name=self._get_job_name(scheduler.task_name),
            schedule=scheduler.schedule,
            time_zone=scheduler.time_zone,
            pubsub_target=PubsubTarget(
                topic_name=app_settings.TOPIC_PATH,
                data=json.dumps(
                    {
                        "task_name": scheduler.task_name,
                        "args": scheduler.args,
                        "kwargs": scheduler.kwargs,
                    }
                ).encode(),
            ),
        )

    def delete_job(self, task_name: str) -> None:
        job_name = self._get_job_name(task_name)
        with contextlib.suppress(NotFound):
            self.client.delete_job(request=DeleteJobRequest(name=job_name))

    def create_job(self, task_name: str, scheduler: SchedulerJob) -> Job:
        job = self.job_from_scheduler(scheduler)
        return self.client.create_job(
            request=CreateJobRequest(parent=self.parent, job=job)
        )

    def update_job(self, task_name: str, scheduler: SchedulerJob) -> Job:
        job = self.job_from_scheduler(scheduler)
        return self.client.update_job(request=UpdateJobRequest(job=job))

    def list_jobs(self) -> ListJobsResponse:
        return self.client.list_jobs(request=ListJobsRequest(parent=self.parent))

    def schedule(self, task_name: str, schedule_config: SchedulerJob) -> Job:
        with contextlib.suppress(AlreadyExists):
            return self.create_job(task_name, schedule_config)

        return self.update_job(task_name, schedule_config)

    def _get_job_name(self, task_name: str) -> str:
        return f"{self.parent}/jobs/{task_name}"

    def _get_parent(self):
        return f"projects/{self.project_id}/locations/{self.location}"
