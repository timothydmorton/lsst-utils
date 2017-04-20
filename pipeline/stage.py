import yaml
import tempfile
import os, re
import logging
import numpy as np
import subprocess
import time
import multiprocessing

from .batch import write_slurm_script, get_job_status

class PipelineStage(object):
    name = None
    id_str_fmt = None
    batch_compatible = True
    single_filter = False
    batch_type = 'slurm'

    _default_kwargs = {}

    def __init__(self, pipeline):
        self.pipeline = pipeline

    @property
    def _id_options(self):
        raise NotImplementedError("Need to define what id keys are valid.")

    @property
    def default_kwargs(self):
        return self._default_kwargs

    def id_str(self, filt=None):
        s = ''
        for key in self._id_options:
            try:
                fmt = '{0}={{0[{0}]}} '.format(key)
                s += fmt.format(self.pipeline)
            except KeyError:
                continue

        if filt is not None:
            s += '--filter={0} '.format(filt)
        return s

    def jobname(self, filt=None):
        s = '{0[ticket]}-{0[field]}-{1}'.format(self.pipeline, self.name)
        if filt is not None:
            s += '-{}'.format(filt)
        return s

    def cmd_str(self, filt=None, test=False, **kwargs):
        cmd = '{0}.py {1[data_root]} --rerun {1.rerun} '.format(self.name, self.pipeline)

        if not isinstance(self, ManualBatchStage):
            cmd += '--job {0} '.format(self.jobname(filt))

        cmd += '--id {0} '.format(self.id_str(filt))

        if hasattr(self, 'selectId_str'):
            cmd += '--selectId {0} '.format(self.selectId_str(filt))

        kws = dict(self.default_kwargs)
        kws.update(self.pipeline['kwargs']['all'])
        kws.update(self.pipeline['kwargs'][self.name])
        kws.update(kwargs)
        for kw, val in kws.items():
            if val is False:
                continue

            # skip using "time" keyword if writing batch script manually
            if isinstance(self, ManualBatchStage) and kw == 'time':
                continue

            cmd += '--{0}'.format(kw)
            if val != '' and val is not True:
                # ensure quotation marks where necessary
                if isinstance(val, basestring):
                    val = '"{0}"'.format(val)

                cmd += '={0} '.format(val)
                if kw == 'nodes':
                    cmd += '--procs {0[cores_per_node]} '.format(self.pipeline)
            else:
                cmd += ' '

        if test:
            cmd = cmd.replace('"', "'")
            np.random.seed()
            jobid = np.random.randint(10000)
            sleep = np.random.random()*2 + 2
            cmd = 'echo "{0}\nbatch job {1}; sleep {2}"'.format(cmd, jobid, sleep)
            print(cmd)

        return cmd
    
    def submit_cmd(self, filt=None, test=False):
        """
        """
        cmd = self.cmd_str(filt, test=test)
        return cmd

    def _get_dependent_jobids(self, filt=None):
        job_ids = self.pipeline.job_ids
        id_depends = []
        if hasattr(self, 'depends'):
            for key,i in job_ids.items():
                # If we know it's done, then skip.
                if i in self.pipeline.complete_job_ids:
                    continue

                for d in self.depends:
                    m = re.match(d, key)
                    if m:
                        if filt is not None:
                            if (key == '{0}-{1}'.format(d, filt) or
                                key == d):
                                id_depends.append(i)
                        else:
                            id_depends.append(i)
        return id_depends
    

    def _wait_for_dependencies(self, filt=None, test=False):
        # Make sure all dependencies are satisfied
        id_depends = self._get_dependent_jobids(filt=filt)
        
        if len(id_depends) > 0:
            name = self.name
            if filt is not None:
                name += ' {0}'.format(filt)
            msg = '{0} waiting for jobids {1}...'.format(name, id_depends)
            print(msg)

        while len(id_depends) > 0:
            completed = []
            for jid in id_depends:
                if test:
                    if np.random.random() < 0.3:
                        status = 'COMPLETED'
                    else:
                        status = 'RUNNING'
                else:
                    status = get_job_status(jid)

                if status in ('RUNNING', 'PENDING'):
                    continue
                elif status == 'COMPLETED':
                    print('jobid {0} completed. '.format(jid))
                    completed.append(jid)
                    self.pipeline.complete_job_ids.append(jid)
                elif status in ('FAILED', 'CANCELLED'):
                    time.sleep(5)
                    status = get_job_status(jid)
                    if status in ('FAILED', 'CANCELLED'):
                        raise RuntimeError('Unexpected status: Job {0} is {1}.'.format(jid, status))
                    else:
                        continue

                else:
                    logging.warning('Unknown status for {0}: {1}.'.format(jid, status))

            for jid in completed:
                id_depends.remove(jid)
            time.sleep(2)

    def submit_job(self, filt=None, test=False):
        """Submits job; returns jobid
        """
        self._wait_for_dependencies(filt=filt, test=test)

        cmd = self.submit_cmd(filt, test=test)
        
        output = subprocess.check_output(cmd, shell=True)
        m = re.search('batch job (\d+)', output)
        if not m:
            logging.error('Cannot find job id: {0}'.format(output))
            logging.error('Command submitted: {0}'.format(cmd))
            raise RuntimeError('Cannot find job id.')
        jobid = int(m.group(1))
        return jobid


class ManualBatchStage(PipelineStage):
    _default_time = 1200
    _override_batch_options = {}

    def write_batch_script(self, filt=None, test=False, **kwargs):
        jobname = self.jobname(filt)
        batchdir = self.pipeline.output_dir
        if not os.path.exists(batchdir):
            os.makedirs(batchdir)
        filename = os.path.join(batchdir, '{0}.sh'.format(jobname))

        try:
            time = self.pipeline['kwargs'][self.name]['time'] / 60 # slurm takes minutes
        except KeyError:
            time = self._default_time / 60

        if time < 65:
            time = 65

        batch_options = {
            'job-name':jobname,
            'output':'{0}/{1}.log'.format(batchdir, jobname),
            'time':time,
            'nodes':1,
            'ntasks-per-node':self.pipeline['cores_per_node'],
        }
        batch_options = dict(batch_options, **self._override_batch_options)
        batch_options = dict(batch_options, **kwargs)
        
        write_slurm_script(filename, self.cmd_str(filt=filt, test=test), **batch_options)

        self.batchfile = filename
        self.logfile = batch_options['output']
        return filename

    def submit_cmd(self, filt=None, test=False):
        batchfile = self.write_batch_script(filt, test=test)
        cmd = 'sbatch {0} '.format(batchfile)
        return cmd
    
class BatchStage(PipelineStage):

    @property
    def _default_kwargs(self):
        kws = {'mpiexec':'-bind-to socket',
               'batch-type':self.batch_type}

        kws['batch-output'] = self.pipeline.output_dir
        return kws


class SingleFrameDriverStage(BatchStage):
    name = 'singleFrameDriver'
    id_str_fmt = 'ccd={0[ccd]} '
    single_filter = True
    _id_options = ('ccd', 'visit')

class MakeDiscreteSkyMapStage(ManualBatchStage):
    name = 'makeDiscreteSkyMap'
    depends = ('singleFrameDriver', )
    _override_batch_options = {'ntasks-per-node':1}

    @property
    def skymap(self):
        try:
            return self.pipeline['skymap']
        except KeyError:
            return None
        
    def _add_dependency(self, cmd, ids):
        if self.skymap is not None:
            return cmd
        else:
            return super(MakeDiscreteSkyMapStage, self)._add_dependency(cmd, ids)

    def cmd_str(self, filt=None, test=False):
        skymap_file = self.skymap
        if skymap_file is None:
            raise ValueError("No precomputed skymap provided!  " +
                             "Need to implement actual skymap creation to continue.")

        target_dir = '{0.pipeline.rerun_dir}/deepCoadd/'.format(self)
        cmd = 'mkdir -p {0}; ln -sf {1.skymap} {0} '.format(target_dir, self)
        return cmd

class MosaicStage(ManualBatchStage):
    name = 'mosaic'
    _id_options = ('tract', 'patch', 'ccd')
    depends = ('singleFrameDriver', 'makeDiscreteSkyMap')
    single_filter = True

class CoaddDriverStage(BatchStage):
    name = 'coaddDriver'
    _id_options = ('tract', 'patch')
    depends = ('mosaic')
    single_filter = True

    def selectId_str(self, filt=None):
        s = 'ccd={0[ccd]} '.format(self.pipeline)
        s += 'filter={0} '.format(filt)
        s += 'visit={0} '.format(self.pipeline['visit'][filt])
        return s

class MultiBandDriverStage(BatchStage):
    name = 'multiBandDriver'
    depends = ('coaddDriver')
    _id_options = ('tract', 'patch')

    def id_str(self, filt=None):
        all_filters = '^'.join(self.pipeline.filters)
        return super(MultiBandDriverStage, self).id_str(filt=all_filters)
