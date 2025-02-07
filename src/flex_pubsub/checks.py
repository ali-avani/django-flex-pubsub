from django.core.checks import register

from .app_settings import app_settings
from .subscription import SubscriptionBase
from .tasks import task_registry


@register()
def sync_scheduler_configs(app_configs, **kwargs):
    if app_settings.SCHEDULER_INITIAL_SYNC:
        task_registry.sync_registered_jobs()
    SubscriptionBase.validate_chosen_subscriptions()
    return []
