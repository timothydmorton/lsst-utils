import yaml
import tempfile
import os, re
import logging
import numpy as np
import subprocess
import time
import multiprocessing
import socket

from pkg_resources import resource_filename

from .stage import SingleFrameDriverStage, MakeDiscreteSkyMapStage, MosaicStage
from .stage import CoaddDriverStage, MultiBandDriverStage

def read_field_config(field):
    filename = resource_filename('pipeline', os.path.join('fields',
                                              '{}.yaml'.format(field)))
    with open(filename) as fin:
        d = yaml.load(fin)

    skymap_file = resource_filename('pipeline', os.path.join('pipeline','fields',
                                              '{}.skymap'.format(field)))
    if os.path.exists(skymap_file):
        d['skymap'] = skymap_file

    return d

class Pipeline(object):
    def __init__(self, filename):
        self.filename = filename
        self._read_yaml()
        self.job_ids = {}
        self.complete_job_ids = []

    def _read_yaml(self):
        with open(self.filename) as fin:
            self._dict = yaml.load(fin)

        if 'field' in self._dict:
            self._dict.update(read_field_config(self._dict['field']))

    def __getitem__(self, item):
        return self._dict[item]

    @property
    def filters(self):
        return self['visit'].keys()

    @property
    def user(self):
        if 'user' in self._dict:
            return self._dict['user']
        else:
            return os.getenv('USER')

    @property
    def rerun_base(self):
        dirname = self.user
        if re.search('lsst-dev', socket.gethostname()):
            dirname = os.path.join('private', dirname)
        return dirname

    @property
    def rerun_unique(self):
        if 'rerun' in self._dict:
            return self['rerun']
        else:
            return '{0[ticket]}/{0[field]}'.format(self)

    @property
    def rerun(self):
        return '{0.rerun_base}/{0.rerun_unique}'.format(self)

    @property
    def rerun_dir(self):
        return '{0[data_root]}/rerun/{0.rerun}'.format(self)

    @property
    def output_dir(self):
        m = re.search('(.*)\.ya?ml', self.filename)
        return '{0}_output'.format(m.group(1))
    
    @property
    def stages(self):
        stages = [eval('{}Stage(self)'.format(s[0].upper()+s[1:])) for s in self['pipeline']]
        return stages
    
    @property
    def commands(self):
        """List of commands.  NOT the exact same as the submit commands.
        """
        commands = []
        for stage in self.stages:
            if stage.single_filter:
                commands += [stage.cmd_str(f) for f in self.filters]
            else:
                commands.append(stage.cmd_str())
        return commands

    def run(self, test=False, parallel=True):
        # Should launch in parallel with Nfilters processes? 
            
        self.job_ids = {}
        self.complete_job_ids = []
        for stage in self.stages:
            if stage.single_filter:
                try:
                    filters = self['kwargs']['filters']
                except KeyError:
                    filters = self.filters
                if parallel:
                    pool = multiprocessing.Pool(len(filters))
                    worker = FilterWorker(stage, test=test)
                    jobids = pool.map(worker, filters)
                    keys = ['{0}-{1}'.format(stage.name, f) for f in filters]
                    for k, j in zip(keys, jobids):
                        self.job_ids[k] = j            
                    pool.close()
                    pool.join()
                else:
                    for filt in filters:
                        jobid = stage.submit_job(filt, test=test)
                        key = '{0}-{1}'.format(stage.name, filt)
                        self.job_ids[key] = jobid
                        print('{0} launched (jobid={1})'.format(key, jobid))
            else:
                key = stage.name
                jobid = stage.submit_job(test=test)
                self.job_ids[key] = jobid
                print('{0} launched (jobid={1})'.format(key, jobid))
                
class FilterWorker(object):
    def __init__(self, stage, test=False):
        self.stage = stage
        self.test = test
        
    def __call__(self, filt):
        time.sleep(np.random.random()*5)
        jobid = self.stage.submit_job(filt, test=self.test)
        print('{0} launched for {1} (jobid={2})'.format(self.stage.name, filt, jobid))
        return jobid
