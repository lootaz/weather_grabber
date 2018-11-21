import datetime
import logging
import os

import pyexcel
import requests
import tzlocal
import yarl

logger = logging.getLogger()


class WeatherSpider:
    HEADERS = ("Дата запроса", "Время измерения", "Температура, *C", "Давление, hPa", "Влажность, %")

    def __init__(self, configs):
        self.configs = configs
        self.openweathermap = self.configs.get('openweathermap', {})
        self.cities = self.configs.get('cities', [])

    def check_out(self):
        if not os.path.exists('out'):
            os.mkdir('out')

        self.export_file_path = './out/out.xls'
        if not os.path.exists(self.export_file_path):
            data = {}
            for city in self.cities.values():
                data[city.get('name_ru')] = [self.HEADERS, ]
            pyexcel.save_book_as(
                bookdict=data,
                dest_file_name=self.export_file_path,
                dest_encoding="utf-8"
            )

    def grab(self):
        self.check_out()

        openweathermap = self.configs.get('openweathermap', {})
        params = {}
        params['appid'] = openweathermap.get('appid')
        params['units'] = 'metric'

        book = pyexcel.get_book(file_name=self.export_file_path)
        for city in self.cities.values():
            params['id'] = city.get('id')
            url = yarl.URL(openweathermap.get('base_url')).with_query(params)
            resp = requests.get(url)
            if resp.status_code != 200:
                logger.warning(f"{city.get('name_ru')} [{url}]: {resp.status_code} {resp.reason}")
                continue

            main = resp.json().get('main', {})

            temp = main.get('temp')
            pressure = main.get('pressure')
            humidity = main.get('humidity')
            dt = resp.json().get('dt')

            calc_time = datetime.datetime.fromtimestamp(dt, datetime.timezone.utc)
            now = datetime.datetime.now(tzlocal.get_localzone())

            data = [now.isoformat(),
                    calc_time.isoformat(),
                    temp,
                    pressure,
                    humidity]
            sheet = book[city.get('name_ru')]
            sheet.row += data

            log_msg = f"{city.get('name_ru')}: {dict(zip(self.HEADERS, data))}"
            logger.info(log_msg)

        book.save_as(self.export_file_path)
