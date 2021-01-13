""" Wake up helper """

from threading import Thread, Event
from datetime import timedelta
import time


class Wakeup(Thread):
    """This thread will periodically set the event
    so that all threads wake up and execute the queue"""

    def __init__(self, event: Event, period: timedelta):
        self.event = event
        self.period = period

        super().__init__(daemon=True)

    def run(self):
        while True:
            time.sleep(self.period.total_seconds())
            self.event.set()
