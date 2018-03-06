#! /usr/bin/env python
# -*- coding: utf-8 -*-

#
# AMIに紐ついてないEBS-Snapshotを一括で削除するツール
#

import argparse

import boto3


def main():
    args = parse_args()
    region = args.region
    dryrun = args.dryrun
    account = boto3.client('sts').get_caller_identity()['Account']
    ec2_client = boto3.client('ec2', region_name=region)
    paginator = ec2_client.get_paginator('describe_snapshots')
    for page in paginator.paginate(OwnerIds=[account]):
        for snapshot in page['Snapshots']:
            try:
                snapshot_id = snapshot['SnapshotId']
                ec2_client.delete_snapshot(
                    SnapshotId=snapshot_id, DryRun=dryrun)
                print 'Delete: {}'.format(snapshot['SnapshotId'])
            except Exception as e:
                print 'Cannot delete: {} ({})'.format(snapshot_id, e)
                continue


def parse_args():
    parser = argparse.ArgumentParser(
        description='Delete unused EBS snapshots')
    parser.add_argument('region', help='AWS region', type=str)
    parser.add_argument(
        '--dryrun', help='Run as dry-run mode if set',
        action='store_true', default=False)
    return parser.parse_args()


if __name__ == '__main__':
    main()
