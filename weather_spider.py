import csv
import datetime
import logging

import requests
import yarl

from weather_locals import BASE_URL, APPID

import time
import pytz
import tzlocal

logger = logging.getLogger()


class WeatherSpider:
    def __init__(self, city_name):
        self.city_name = city_name
        self.export_file_name = f"out_{city_name}.csv"

    def grab(self):
        params = {}
        params['q'] = self.city_name
        params['appid'] = APPID
        params['units'] = 'metric'
        url = yarl.URL(BASE_URL).with_query(params)

        resp = requests.get(url)
        main = resp.json().get('main', {})
        temp = main.get('temp', {})
        pressure = main.get('pressure')
        humidity = main.get('humidity')
        dt = resp.json().get('dt')
        calc_time = datetime.datetime.fromtimestamp(dt, datetime.timezone.utc)
        now = datetime.datetime.now(tzlocal.get_localzone())

        data = {
            "Дата запроса": now.isoformat(),
            "Время измерения": calc_time.isoformat(),
            "Температура": temp,
            "Давление": pressure,
            "Влажность": humidity
        }
        self.export(data)
        logger.info(data)

    def export(self, data):
        with open(self.export_file_name, 'a', encoding='utf-8', newline='') as csvfile:
            weather_writer = csv.writer(csvfile)
            weather_writer.writerow(data.values())
