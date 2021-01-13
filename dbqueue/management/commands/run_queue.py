""" Long-running async task runner """

from threading import Event

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.conf import settings

from dbqueue.runner import NotificationThread

# Allow Postgres and PostGIS
ALLOWED_ENGINES = (
    "django.contrib.gis.db.backends.postgis",
    "django.db.backends.postgresql",
)

# Mapping between Django connection argument names,
# and the args that should be passed to psycopg2.connect(**)
SETTINGS_MAP = {
    "dbname": "NAME",
    "user": "USER",
    "password": "PASSWORD",
    "host": "HOST",
    "port": "PORT",
}


class Command(BaseCommand):
    help = "Runs the async task runner"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--rescan-period",
            type=int,
            default=60,
            help="Forced task queue rescan interval, in seconds",
        )

    def handle(self, *args, **kwargs):
        channel_name = self.get_channel_name()
        database_alias = self.get_database_alias()

        db_settings = settings.DATABASES[database_alias]

        event = Event()

        engine = db_settings["ENGINE"]
        if engine not in ALLOWED_ENGINES:
            raise CommandError(
                "Async task queue only supports the "
                "PostgreSQL and PostGIS database engines"
            )

        conn_args = {}

        # Copy the arguments to psycopg2, but leave the psycopg2 defaults
        # if the Django setting isn't set
        for psycopg2_key, django_key in SETTINGS_MAP.items():
            if db_settings.get(django_key):
                conn_args[psycopg2_key] = db_settings[django_key]

        notification_thread = NotificationThread(
            channel_name=channel_name, conn_args=conn_args, event=event
        )

        notification_thread.start()

        try:
            notification_thread.join()
        except KeyboardInterrupt:
            notification_thread.stop()
            notification_thread.join()

    def get_channel_name(self):
        try:
            return settings.DBQUEUE_CHANNEL_NAME
        except AttributeError:
            return "dbqueue_notifications"

    def get_database_alias(self):
        try:
            return settings.DBQUEUE_DATABASE_ALIAS
        except AttributeError:
            return "default"

    def settings_check(self, settings_dict: dict) -> bool:
        if settings_dict["ENGINE"] not in ("django.db.backends.postgresql_psycopg2",):
            return False
