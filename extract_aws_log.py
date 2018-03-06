#! /usr/bin/env python
# -*- coding: utf-8 -*-

#
# AWS CloudWatch Logsからログストリームの内容を抽出し、
# テキストファイルに保存するツール
#

import argparse
import calendar
import logging
import os
import sys
from datetime import datetime
from itertools import chain
from operator import itemgetter
from pprint import pprint

import boto3

AWS_SESSION = None


def main():
    args = parse_args()

    prepare_session(args.profile, args.region)
    output_filepath = os.path.join(
        args.output_dir,
        args.log_group_name.replace('/', '_') + '.txt')
    extract_log(
        args.log_group_name,
        datetime.strptime(args.from_time + ' UTC', '%Y-%m-%dT%H:%M:%S %Z'),
        datetime.strptime(args.to_time + ' UTC', '%Y-%m-%dT%H:%M:%S %Z'),
        output_filepath,
        args.log_stream_names
    )


def parse_args():
    parser = argparse.ArgumentParser()
    date_help = 'ISO8601 formatted UTC datetime(ex. 2018-03-06T10:41:00)'
    parser.add_argument('log_group_name')
    parser.add_argument('from_time', help=date_help)
    parser.add_argument('to_time', help=date_help)
    parser.add_argument('--output_dir', default='.')
    parser.add_argument(
        '--log_stream_names', nargs='+', metavar='STREAM_NAME', default=[])
    parser.add_argument('--profile', help='AWS profile', default=None)
    parser.add_argument('--region', help='AWS region', default=None)
    if len(sys.argv) <= 1:
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()


def prepare_session(profile, region):
    global AWS_SESSION
    AWS_SESSION = boto3.Session(region_name=region, profile_name=profile)


def extract_log(
        log_group, from_time, to_time, output_filepath, log_stream_name_list):

    cwlogs = AWS_SESSION.client('logs')
    logst_paginator = cwlogs.get_paginator('describe_log_streams')
    from_time_unix = calendar.timegm(from_time.utctimetuple()) * 1000
    logging.info('from_time:%d', from_time_unix)
    to_time_unix = calendar.timegm(to_time.utctimetuple()) * 1000
    logging.info('to_time:%d', to_time_unix)

    if log_stream_name_list:
        log_stream_names = log_stream_name_list
    else:
        logst_iter = logst_paginator.paginate(
            logGroupName=log_group,
            orderBy='LastEventTime',
            descending=True
        )

        log_stream_names = []
        for logst_page in logst_iter:
            log_stream_names.extend([
                log_stream['logStreamName']
                for log_stream in logst_page['logStreams']])

    log_events = []

    if log_stream_names:
        for log_stream_names_group in zip(*[iter(log_stream_names)]*100):
            logev_paginator = cwlogs.get_paginator('filter_log_events')
            logev_iter = logev_paginator.paginate(
                logGroupName=log_group,
                logStreamNames=log_stream_names_group,
                startTime=from_time_unix,
                endTime=to_time_unix
            )
            log_events.extend(chain(*[page['events'] for page in logev_iter]))

    log_events_sorted = sorted(log_events, key=itemgetter('timestamp'))

    with open(output_filepath, 'w') as outfile:
        for line in log_events_sorted:
            outfile.write(
                "{:%Y-%m-%d %H:%M:%S} {}\n".format(
                    datetime.utcfromtimestamp(line['timestamp']/1000), line['message'].rstrip()))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
