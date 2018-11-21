import logging
import logging.config
import sys

import toml

from schedule_service import ScheduleService
from weather_spider import WeatherSpider

logger = logging.getLogger()


class MainWorker:
    def __init__(self, configs):
        period_sec = configs.get('openweathermap', {}).get('period_sec')
        if not period_sec:
            logger.error("Необходим 'period_sec'")
            sys.exit(-1)

        weather_spider = WeatherSpider(configs)

        self.schedules = []
        self.schedules.append(ScheduleService(f"WeatherSpider", period_sec, weather_spider.grab))

    def run(self):
        for schedule in self.schedules:
            schedule.periodic()
            schedule.run()


def init_logging():
    logging.config.fileConfig('./config/logging.conf')


def init_main():
    with open('./config/main.toml', encoding='utf-8') as fin:
        configs = toml.load(fin)
        return configs


if __name__ == '__main__':
    init_logging()
    configs = init_main()

    if not configs.get('openweathermap', {}).get('appid'):
        logger.error("Необходим APPID ключ")
        sys.exit(-1)

    mw = MainWorker(configs)
    mw.run()
