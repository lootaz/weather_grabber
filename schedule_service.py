import sched
from datetime import datetime
import logging


logger = logging.getLogger()


class ScheduleService:
    def __init__(self, name, interval, action):
        self.name = name
        self.interval = interval
        self.action = action
        self.scheduler = sched.scheduler()

    def periodic(self):
        logger.info("[%s] Schedule '%s' executed" % (datetime.utcnow(), self.name))
        self.action()
        self.scheduler.enter(self.interval, 1, self.periodic)
        logger.info("[%s] Schedule '%s' complete" % (datetime.utcnow(), self.name))

    def run(self):
        self.scheduler.run()
    
