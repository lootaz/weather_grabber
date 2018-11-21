import csv
import datetime
import logging

import requests
import tzlocal
import yarl

logger = logging.getLogger()


class WeatherSpider:
    def __init__(self, city_id, appid, base_url):
        self.city_id = city_id
        self.appid = appid
        self.base_url = base_url
        self.export_file_name = f"./out/out_{city_id}.csv"


    def grab(self):
        params = {}
        params['id'] = self.city_id
        params['appid'] = self.appid
        params['units'] = 'metric'
        url = yarl.URL(self.base_url).with_query(params)

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
