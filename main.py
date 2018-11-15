import logging
import logging.config
import sys
from multiprocessing.pool import ThreadPool

import requests
import sched

from schedule_service import ScheduleService
from weather_locals import APPID, PERIOD_SEC
from weather_spider import WeatherSpider

logger = logging.getLogger()


class MainWorker:
    def __init__(self):
        self.pool = ThreadPool(5)

        ws_samara = WeatherSpider("Samara")
        self.schedules = []
        self.schedules.append(ScheduleService("WeatherSpider Samara", PERIOD_SEC, ws_samara.grab))

    def run_schedule(self, schedule):
        schedule.periodic()
        schedule.run()

    def run(self):
        self.pool.map(self.run_schedule, self.schedules)


def init_logging():
    logging.config.fileConfig('logging.conf')


if __name__ == '__main__':
    init_logging()
    if not APPID:
        logger.error("Необходим APPID ключ")
        sys.exit(-1)

    mw = MainWorker()
    mw.run()
