from setuptools import setup, find_packages
import os, sys


def readme():
    with open('readme.md') as f:
        return f.read()

setup(name = "pipeline",
    version = 0.0,
    description = "Launch LSST pipeline runs in one step.",
    long_description = readme(),
    author = "Timothy D. Morton",
    author_email = "tdm@astro.princeton.edu",
    url = "https://github.com/timothydmorton/lsst-utils/pipeline",
    packages = find_packages(),
    scripts = ['scripts/runPipeline.py'],
    package_data = {'pipeline': ['fields/*']},
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Science/Research',
      'Operating System :: OS Independent',
      'Programming Language :: Python',
      'Topic :: Scientific/Engineering :: Astronomy'
      ],
    zip_safe=False
)
