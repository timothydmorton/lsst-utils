import yaml
import tempfile
import os, re
import logging
import numpy as np
import subprocess
import time
import multiprocessing
import socket

from .stage import SingleFrameDriverStage, MakeDiscreteSkyMapStage, MosaicStage
from .stage import CoaddDriverStage, MultiBandDriverStage

class Pipeline(object):
    def __init__(self, filename):
        self.filename = filename
        self._read_yaml()
        self.job_ids = {}
        self.complete_job_ids = []

    def _read_yaml(self):
        with open(self.filename) as fin:
            self._dict = yaml.load(fin)

    def __getitem__(self, item):
        return self._dict[item]

    @property
    def filters(self):
        return self['visit'].keys()

    @property
    def rerun(self):
        if 'rerun' in self._dict:
            return self['rerun']
        else:
            dirname = '{0[user]}/{0[ticket]}/{0[field]}'.format(self)
            if re.search('lsst-dev', socket.gethostname()):
                dirname = os.path.join('private', dirname)
            return dirname

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
                if parallel:
                    pool = multiprocessing.Pool(len(self.filters))
                    worker = FilterWorker(stage, test=test)
                    jobids = pool.map(worker, self.filters)
                    keys = ['{0}-{1}'.format(stage.name, f) for f in self.filters]
                    for k, j in zip(keys, jobids):
                        self.job_ids[k] = j            
                    pool.close()
                    pool.join()
                else:
                    for filt in self.filters:
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
