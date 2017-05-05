import sys
import argparse

from pipeline.batch import get_pipeline_status

parser = argparse.ArgumentParser('Check status of pipeline')
parser.add_argument('name', help='yaml file basename')
parser.add_argument('--all', action='store_true')

args = parser.parse_args()

status = get_pipeline_status(args.name)

if args.all:
    print(status)
else:
    print(status[-1])

    