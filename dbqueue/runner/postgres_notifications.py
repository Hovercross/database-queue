""" Postgres async handler """

from threading import Thread, Event
from logging import getLogger
import select
import os
import struct

import psycopg2

log = getLogger(__name__)


class NotificationThread(Thread):
    """ Thread that gets it's own Postgres connection to grab all the events """

    # We aren't using the Django database handling here,
    # because we don't want the underlying connections to be closed or managed.
    # Communication between this and the task runner will be done via an event

    def __init__(self, conn_args: dict, channel_name: str, run_event: Event):
        self._conn_args = conn_args
        self.channel_name = channel_name
        self.run_event = run_event
        self.exiting = False
        self.pipe = os.pipe()  # Trigger the select when we're done

        super().__init__(name="Postgres async notifications")

    def run(self):
        log.info("Connecting to database")
        conn = psycopg2.connect(**self._conn_args)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        log.info("Connected to database")

        log.debug("Getting cursor")
        cur = conn.cursor()
        log.debug("Got cursor")

        log.info("Listening for notifications on %s", self.channel_name)

        # TODO: I want to parameterize this, but it seems to throw a syntax error
        cur.execute(f"LISTEN {self.channel_name}")

        log.debug("Executing listen")

        while not self.exiting and not conn.closed:
            log.debug("Beginning select")

            if select.select([conn, self.pipe[0]], [], [], 30) == ([], [], []):
                log.debug("Select timeout")
            else:
                conn.poll()
                log.debug("Received notification")

                # Because the notification is just a wake-up for all threads,
                # we don't care about the individual messages - just unlock the
                # event after removing all the notifications.
                while conn.notifies:
                    # This can probably be done more efficiently, but this was the
                    # example from the psycopg2 documentation
                    conn.notifies.pop(0)

                if not self.exiting:
                    log.debug("setting event")
                    self.run_event.set()
                    log.debug("event set")

        # Clean up after ourselves
        log.debug("Exiting notification thread")
        if not conn.closed:
            log.debug("Closing database connection")
            conn.close()
            log.info("Database connection closed")

        log.info("notifiction thread finished")

    def stop(self):
        self.exiting = True
        os.write(self.pipe[1], struct.pack("B", 0))

    def handle(self):
        """ Send out the appropriate notification handlers """
