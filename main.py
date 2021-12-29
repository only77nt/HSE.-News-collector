# Документация к API: https://hackernews.api-docs.io
# Сайт: https://news.ycombinator.com/news
# -*- coding: utf-8 -*-

import html
import http.client
import urllib.request
import datetime
import json
import requests
from bs4 import BeautifulSoup

# Константы, можно вынести в отдельный конфиг файл (например, .yaml)
DB_CONNECTION_STRING = "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
DB_NAME = 'site_parser'
SITE_URL = "hacker-news.firebaseio.com"
BEGIN = datetime.datetime(2021, 10, 1, 0, 0).timestamp()
END = datetime.datetime(2021, 10, 10, 0, 0).timestamp()


# Собираем новости за период (период задаётся в константах)
def collect_news():
    news = []

    mid_news_id = find_mid_news_id(0, get_max_item_id())

    it1 = mid_news_id - 1
    it2 = mid_news_id + 1

    # Тестовый кейс (т.к. новостей много и загружать через API все достаточно долго)
    news.append(get_info_by_id(it1))
    news.append(get_info_by_id(it2))
    news.append(get_info_by_id(190))

    '''
    # Просматриваем новости, которые раньше найденной
    while 1:
        new = get_info_by_id(it1)

        if new["time"] < begin:
            break

        print(begin, new["time"])

        news.append(new)
        it1 -= 1

    # Просматриваем новости, которые позже найденной
    while 1:
        new = get_info_by_id(it2)

        if new["time"] > end:
            break

        print(end, new["time"])

        news.append(new)
        it2 += 1
    '''

    print("Новости за промежуток времени собраны")

    return news


# Быстрым поиском находим новость из заданного временного промежутка
def find_mid_news_id(first_item_id, last_item_id):
    mid = (first_item_id + last_item_id) // 2

    mid_time = get_info_by_id(mid)["time"]

    if BEGIN < mid_time < END:
        print("Найдена новость из промежутка")

        return mid
    else:
        if mid_time > END:
            last_item_id = mid - 1
        else:
            first_item_id = mid + 1

        return find_mid_news_id(first_item_id, last_item_id)


# Получаем id с максимальным значением при помощи API
def get_max_item_id():
    conn = http.client.HTTPSConnection(SITE_URL)

    payload = "{}"

    conn.request("GET", "/v0/maxitem.json?print=pretty", payload)

    res = conn.getresponse()
    data = res.read()

    print("Найден id новости с максимальным значением")

    return int(data)


# Получает информацию о новости при помощи API (по id новости)
def get_info_by_id(id):
    conn = http.client.HTTPSConnection(SITE_URL)

    payload = "{}"

    conn.request("GET", f"/v0/item/{id}.json?print=pretty", payload)

    res = conn.getresponse()
    data = res.read()

    obj = json.loads(data)

    return obj


# Достаёт картинки с сайта
def get_site_imgs(site_url):
    response = requests.get(site_url)

    soup = BeautifulSoup(response.text, 'html.parser')
    img_tags = soup.find_all('img')

    return [img['src'] for img in img_tags]


def get_database():
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    return client[DB_NAME]


# Основная функция, вызывающаяся при старте программы
def main():
    code = urllib.request.urlopen("https://" + SITE_URL).getcode()
    if code != 200:
        print("Сайт недоступен!")
        return

    dbname = get_database()

    job_collection = dbname["job"]
    story_collection = dbname["story"]
    comment_collection = dbname["comment"]
    poll_collection = dbname["poll"]
    pollopt_collection = dbname["pollopt"]
    images_collection = dbname["images"]

    for obj in collect_news():
        # Есть 5 типов контента: job, story, comment, poll, pollopt
        # У каждого из них различный набор полей, но поле "type" всегда задано
        # Проверяя это поле, можно определить что перед нами и что с ним делать
        print(obj["type"])

        id = obj["id"]

        obj["time"] = datetime.datetime.fromtimestamp(obj["time"])

        if "text" in obj:
            obj["text"] = html.unescape(obj["text"])

        # Записываем информацию в базу
        if obj["type"] == "job":
            job_collection.update_one({"_id": id}, {"$set": obj}, upsert=True)

        if obj["type"] == "story":
            story_collection.update_one({"_id": id}, {"$set": obj}, upsert=True)

        if obj["type"] == "comment":
            comment_collection.update_one({"_id": id}, {"$set": obj}, upsert=True)

        if obj["type"] == "poll":
            poll_collection.update_one({"_id": id}, {"$set": obj}, upsert=True)

        if obj["type"] == "pollopt":
            pollopt_collection.update_one({"_id": id}, {"$set": obj}, upsert=True)

        if "url" in obj:
            for img_url in get_site_imgs(obj["url"]):
                images_collection.update_one({"url": img_url}, {"$set": {"news_id": id}}, upsert=True)

    print("Конец работы программы")


# Функция инициализации программы
if __name__ == '__main__':
    main()
