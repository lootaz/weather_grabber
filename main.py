import logging
import logging.config
import sys
from multiprocessing.pool import ThreadPool

import toml

from schedule_service import ScheduleService
from weather_spider import WeatherSpider

logger = logging.getLogger()


class MainWorker:
    def __init__(self, configs):

        city_ids = configs.get('city_ids')
        period_sec = configs.get('period_sec')
        appid = configs.get('appid')
        base_url = configs.get('base_url')

        self.pool = ThreadPool(len(city_ids))
        self.schedules = []
        for city_id in city_ids:
            grabber = WeatherSpider(city_id, appid, base_url)
            self.schedules.append(ScheduleService(f"WeatherSpider [{city_id}]", period_sec, grabber.grab))

    def run_schedule(self, schedule):
        schedule.periodic()
        schedule.run()

    def run(self):
        self.pool.map(self.run_schedule, self.schedules)


def init_logging():
    logging.config.fileConfig('./config/logging.conf')


def init_main():
    with open('./config/main.toml') as fin:
        configs = toml.load(fin)
        return configs


if __name__ == '__main__':
    init_logging()
    configs = init_main()
    openweathermap = configs.get('openweathermap')

    if not openweathermap.get('appid'):
        logger.error("Необходим APPID ключ")
        sys.exit(-1)

    mw = MainWorker(openweathermap)
    mw.run()
