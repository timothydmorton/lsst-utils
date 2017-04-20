import sys

from pipeline import Pipeline

test = '--test' in sys.argv

configfile = sys.argv[1]

pipe = Pipeline(configfile)
pipe.run(test=test)