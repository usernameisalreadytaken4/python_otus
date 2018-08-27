#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import gzip
from datetime import datetime
import logging

# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';



config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "reports",
    "LOG_DIR": "log",
    # 'CURRENT_DAY': datetime.today().strftime('%Y%m%d'),
    'CURRENT_DAY': '20170630'
}

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname).1s %(message)s', filename='worklog_%s.log' % config.get('CURRENT_DAY'))

report_path = os.path.join(os.path.dirname(__file__), config.get('REPORT_DIR'))
log_path = os.path.join(os.path.dirname(__file__), config.get('LOG_DIR'))
RES = {}
REPORT_SIZE_READY = []


def aggregate(url, weight):
    """
    URL: {count, count_perc, time_sum, time_perc, time_avg, time_max, time_med}
    """

    if url not in RES:
        RES.update({url: {
            'count': 1,
            'count_perc': None,
            'time_sum': weight,
            'time_perc': None,
            'time_avg': None,
            'time_max': weight,
            'time_med': weight
        }})
    else:
        RES[url]['count'] += 1
        RES[url]['time_sum'] += weight
        RES[url]['time_max'] = weight if weight > RES[url]['time_max'] else RES[url]['time_max']
        RES[url]['time_med'] = (RES[url]['time_med'] + weight) / 2


def calculate(ALL_TIME, COUNT):
    '''
    calculate result
    '''
    logging.info('CALCULATING...')
    ALL_TIME_one_percent = ALL_TIME / 100
    COUNT_one_percent = COUNT / 100

    for url in RES:
        RES[url]['count_perc'] = str(float(RES[url]['count']) / float(COUNT_one_percent))
        RES[url]['time_avg'] = str(RES[url]['time_sum'] / RES[url]['count'])
        RES[url]['time_perc'] = str(RES[url]['time_sum'] / ALL_TIME_one_percent)
        RES[url]['url_name'] = url
        REPORT_SIZE_READY.append(RES[url])


def get_today_log():
    for log in os.listdir(log_path):
        if config.get('CURRENT_DAY') in log:
            if 'gz' in log:
                _file = gzip.open(os.path.join(log_path, log), 'rb')
                # logging.info('OPEN gzip LOGFILE: %s' % log)
                for line in _file.readlines():
                    yield line
                _file.close()
            else:
                with open(os.path.join(log_path, log), 'rb') as _file:
                    logging.info('OPEN text LOGFILE: ', log)
                    for line in _file.readlines():
                        yield line
                    _file.close()


def create_report():
    with open(os.path.join(report_path, 'report-%s.csv' % config.get('CURRENT_DAY')), 'wb') as report:
        logging.info('CREATING REPORT FILE WITH NAME: ', report.name)
        report.write('URL,count,count_perc,time_sum,time_perc,time_avg,time_max,time_med\n')
        for parsed_url in sorted(REPORT_SIZE_READY, key=lambda x: x['time_sum'], reverse=True)[:config.get('REPORT_SIZE')]:
            report.write('%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                parsed_url['url_name'], parsed_url['count'], parsed_url['count_perc'], parsed_url['time_sum'],
                parsed_url['time_perc'], parsed_url['time_avg'], parsed_url['time_max'], parsed_url['time_med']
            ))
        report.close()


def main():

    logging.info('start working %s ' % config.get('CURRENT_DAY'))

    if not os.path.exists(report_path):
        logging.info('REPORTS DIRECTORY CREATED')
        os.mkdir(report_path)

    for log in os.listdir(report_path):
        if config.get('CURRENT_DAY') in log:
            logging.error('REPORT IS ALREADY CREATED')
            raise StandardError

    ALL_TIME = 0.0
    COUNT = 0

    logging.info('AGGREGATING...')

    for line in get_today_log():
        line = line.split()
        url = line[6]
        if url.isdigit():
            logging.info('get fake url: %s' % url)
            continue
        weight = float(line[-1])
        if weight == 0.0:
            logging.info('get zero time on url: %s' % url)
            continue
        aggregate(url, weight)
        ALL_TIME += weight
        COUNT += 1
      
    logging.info('ALL_TIME: ', ALL_TIME)
    logging.info('REQUESTS COUNT: ', COUNT)
    calculate(ALL_TIME, COUNT)
    create_report()
    logging.info('... END ...')
    logging.info('REPORT SIZE: %s' % config.get('REPORT_SIZE'))


if __name__ == "__main__":
    main()
