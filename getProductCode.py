import json
import multiprocessing
import os
import sys
import time
import re
import urllib
import logging

import requests


def crawlComments(categoryCode):
    session = requests.Session()
    authorization = "Bearer 9z5uVWF1KqCq5KQdBoilwKJDlp8p7a"
    url = "https://gateway.glowpick.com/api/v2/categories/{}/products".format(categoryCode)
    headers = {
        'authorization': authorization,
        'x-glowpick-method': "GET",
        'x-glowpick-query': "%7B%22gender%22%3A%22%22%2C%22skin_type%22%3A%22%22%2C%22age%22%3A%22%22%2C%22cursor%22%3Anull%2C%22id%22%3A%2243%22%2C%22brand_category_id%22%3A%22%22%2C%22min_price%22%3Anull%2C%22max_price%22%3Anull%2C%22order%22%3A%22review_desc%22%2C%22limit%22%3A1000%7D",
    }

    try:
        products = session.post(url, headers=headers, timeout=5).json()
    except requests.exceptions.RequestException as re:
        logging.warning("{} Category Exception :: {} --> {}".format(os.getpid(), next_url, re))
        return

    for p in products['products']:
        next_url = "https://gateway.glowpick.com/api/v2/products/{}/reviews".format(p['product_id'])
        x_glow_pick_query = "%7B%22gender%22%3A%22all%22%2C%22age%22%3A%22all%22%2C%22skin_type%22%3A%22all%22%2C%22rating%22%3A%22all%22%2C%22order%22%3A%22create_date_desc%22%2C%22contents%22%3A%22%22%2C%22cursor%22%3A%22%22%2C%22limit%22%3A100%7D"

        while next_url:
            headers = {
                'authorization': authorization,
                'x-glowpick-method': "GET",
                'x-glowpick-query': x_glow_pick_query
            }
            try:
                comments = session.post(next_url, headers=headers, timeout=5).json()
            except requests.exceptions.RequestException as re:
                logging.warning("{} Product Exception :: {} --> {}".format(os.getpid(), next_url, re))
                break

            localList = list()
            for c in comments['reviews']:
                for s in list(filter(lambda e: len(e) != 0, c['contents'].split('\n'))):
                    localList.append("__label__{} {}".format(c['rating'], s))
            taskQueue.put(localList)

            if comments['paging']:
                temp = json.loads(urllib.parse.unquote(x_glow_pick_query))
                temp['cursor'] = comments['paging']['next']
                x_glow_pick_query = urllib.parse.quote(json.dumps(temp))
            else:
                next_url = None

    return


def initializer(q):
    global taskQueue
    taskQueue = q


def writer():
    with open('result.txt', 'w') as result:
        while True:
            try:
                lines = taskQueue.get(timeout=30)
            except:
                logging.info("queue timeout.")
            for l in lines:
                result.write(l + '\n')


if __name__ == "__main__":
    logging.basicConfig(filename='event.log', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.info('start logging')

    headers = {
        'authorization': "Bearer 9z5uVWF1KqCq5KQdBoilwKJDlp8p7a",
        'x-glowpick-method': "GET",
        'x-glowpick-query': "%7B%22gender%22%3A%22%22%2C%22skin_type%22%3A%22%22%2C%22age%22%3A%22%22%2C%22cursor%22%3Anull%2C%22id%22%3A%2243%22%2C%22brand_category_id%22%3A%22%22%2C%22min_price%22%3Anull%2C%22max_price%22%3Anull%2C%22order%22%3A%22rank%22%2C%22limit%22%3A200%7D",
    }

    categoryID = list()
    response = requests.request("POST", "https://gateway.glowpick.com/api/v2/categories", headers=headers).json()
    for c in response['categories']:
        for sc in c['sub_categories']:
            categoryID.append(sc['sub_category_id'])

    print('cate :: ' + str(len(categoryID)))
    cpu_count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cpu_count, initializer=initializer, initargs=(multiprocessing.Queue(), ))
    print('pool size : ' + str(cpu_count))
    pool.apply_async(writer)
    pool.map_async(crawlComments, categoryID)
    pool.close()
    pool.join()
    print('all done')
