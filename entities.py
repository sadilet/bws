class Task:
    ID = 0

    QUEUED = 1
    PROGRESS = 2
    ERROR = 3
    READY = 4

    STATES = {
        QUEUED: "queued",
        PROGRESS: "progress",
        ERROR: "error",
        READY: "ready",
    }

    def __init__(self, concurrency, url):
        Task.ID += 1
        self.id = Task.ID
        self.state = self.QUEUED
        self.concurrency = concurrency
        self.url = url
