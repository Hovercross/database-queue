""" Multiple event proxy """

from typing import List
from threading import Thread, Event


class EventProxy(Thread):
    """Thread that can listen on one event, and then set another.
    Useful if you have N events to listen on, and then want to
    set one event if any of those events fire"""

    def __init__(self, listen: Event, set: Event):
        self.listen = listen
        self.set = set

        super().__init__(daemon=True, name="Event proxy")

    def run(self):
        self.listen.wait()
        self.set.set()


class MultiEventWaiter(Thread):
    """ Wait for multiple events, and then set an event when they are all finished """

    def __init__(self, wait_events: List[Event], on_finished: Event):
        self.wait_events = wait_events
        self.on_finished = on_finished

        super().__init__(daemon=True)

    def run(self):
        for event in self.wait_events:
            event.wait()

        self.on_finished.set()


class WaitEvent(Thread):
    """ Thread that can wait for another thread to finish, then set an event """

    def __init__(self, listen_thread: Thread, on_join: Event):
        self.listen = listen_thread
        self.on_join = on_join

        super().__init__(daemon=True, name="Thread waiter")

    def run(self):
        self.listen.join()
        self.on_join.set()
