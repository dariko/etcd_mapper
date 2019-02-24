from threading import Thread, Event


class StoppableThread(Thread):
    def __init__(self, *args, **kwargs):
        self.quit_event = Event()
        super().__init__(*args, **kwargs)

    def stop(self, wait=True):
        if not self.isAlive():
            return
        self.quit_event.set()
        self.join()
        self.quit_event.clear()
