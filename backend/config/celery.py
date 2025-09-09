import os
from celery import Celery
from django.conf import settings

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.development')

app = Celery('calendly_clone')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# Optional configuration, see the application user guide.
app.conf.update(
    task_track_started=True,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    result_expires=3600,
    timezone='UTC',
    enable_utc=True,
    beat_schedule={
        'check-password-expiries': {
            'task': 'apps.users.tasks.check_password_expiries_and_warn',
            'schedule': 86400.0,  # Run daily (24 hours)
        },
        'cleanup-expired-grace-periods': {
            'task': 'apps.users.tasks.cleanup_expired_grace_periods',
            'schedule': 3600.0,  # Run hourly
        },
        'cleanup-expired-tokens': {
            'task': 'apps.users.tasks.cleanup_expired_tokens',
            'schedule': 86400.0,  # Run daily
        },
        'unlock-locked-accounts': {
            'task': 'apps.users.tasks.unlock_locked_accounts',
            'schedule': 1800.0,  # Run every 30 minutes
        },
        'process-dirty-cache-flags': {
            'task': 'apps.availability.tasks.process_dirty_cache_flags',
            'schedule': 300.0,  # Run every 5 minutes
        },
        'monitor-cache-performance-detailed': {
            'task': 'apps.availability.tasks.monitor_cache_performance_detailed',
            'schedule': 3600.0,  # Run every hour
        },
        'sync-all-calendar-integrations': {
            'task': 'apps.integrations.tasks.sync_all_calendar_integrations',
            'schedule': 900.0,  # Run every 15 minutes
        },
        'refresh-expired-tokens': {
            'task': 'apps.integrations.tasks.refresh_expired_tokens',
            'schedule': 3600.0,  # Run every hour
        },
        'cleanup-old-integration-logs': {
            'task': 'apps.integrations.tasks.cleanup_old_integration_logs',
            'schedule': 86400.0,  # Run daily
        },
        'process-scheduled-notifications': {
            'task': 'apps.notifications.tasks.process_scheduled_notifications',
            'schedule': 300.0,  # Run every 5 minutes
        },
        'send-booking-reminders': {
            'task': 'apps.notifications.tasks.send_booking_reminders',
            'schedule': 600.0,  # Run every 10 minutes
        },
        'send-daily-agenda': {
            'task': 'apps.notifications.tasks.send_daily_agenda',
            'schedule': 3600.0,  # Run every hour (will check if it's the right time)
        },
        'cleanup-old-notification-logs': {
            'task': 'apps.notifications.tasks.cleanup_old_notification_logs',
            'schedule': 86400.0,  # Run daily
        },
        'monitor-notification-failures': {
            'task': 'apps.notifications.tasks.monitor_notification_failures',
            'schedule': 3600.0,  # Run every hour
        },
        'retry-failed-notifications': {
            'task': 'apps.notifications.tasks.retry_failed_notifications',
            'schedule': 1800.0,  # Run every 30 minutes
        },
    },
)

@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')