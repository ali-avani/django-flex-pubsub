from typing import Any

from flex_pubsub.app_settings import app_settings
from flex_pubsub.backends import BaseBackend
from django.utils import timezone
from flex_pubsub.tasks import task_registry
from flex_pubsub.types import CallbackContext, RequestMessage

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Starts the subscriber to listen for messages and execute tasks."

    def message_callback(self, context: CallbackContext) -> None:
        try:
            raw_message = context.raw_message
            ack = context.ack
            data = RequestMessage.model_validate_json(raw_message)

            task = task_registry.get_task(data.task_name)
            t_args = data.args
            t_kwargs = data.kwargs

            if set(task.subscriptions).issubset(set(app_settings.SUBSCRIPTIONS)):
                ack()
                task(*t_args, **t_kwargs)
        except Exception as e:
            self.stderr.write(f"Error processing message: {str(e)}")

    def display_registered_tasks(self):
        self.stdout.write("Registered tasks:")
        for task_name in task_registry.get_all_tasks():
            self.stdout.write(
                f"  - {task_name} ({schedule if (schedule:=task_registry.get_schedule_config(task_name)) else 'No schedule'})"
            )

    def handle(self, *args: Any, **options: Any) -> None:
        backend_class = app_settings.BACKEND_CLASS
        backend: BaseBackend = backend_class()

        self.display_registered_tasks()
        self.stdout.write("Starting subscriber...")
        backend.subscribe(self.message_callback)