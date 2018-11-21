import logging
import sched
from datetime import datetime

logger = logging.getLogger()


class ScheduleService:
    def __init__(self, name, interval, action):
        self.name = name
        self.interval = interval
        self.action = action
        self.scheduler = sched.scheduler()

    def periodic(self):
        logger.info("[%s] Schedule '%s' begin..." % (datetime.utcnow(), self.name))
        self.action()
        self.scheduler.enter(self.interval, 1, self.periodic)
        logger.info("[%s] Schedule '%s' completed!" % (datetime.utcnow(), self.name))

    def run(self):
        self.scheduler.run()
    
