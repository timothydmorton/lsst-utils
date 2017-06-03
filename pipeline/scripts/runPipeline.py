import sys
import argparse

from pipeline import Pipeline

parser = argparse.ArgumentParser('Run a pipeline')
parser.add_argument('configfile', help='yaml file')
parser.add_argument('--test', action='store_true')
parser.add_argument('--serial', action='store_true')

args = parser.parse_args()

if not args.configfile.endswith('.yaml'):
    file = args.configfile + '.yaml'
else:
    file = args.configfile

pipe = Pipeline(file)
pipe.run(test=args.test, parallel=not args.serial)

