import yaml
import tempfile
import os, re
import logging
import numpy as np
import subprocess

class PipelineStage(object):
    name = None
    id_str_fmt = None
    batch_compatible = True
    single_filter = False

    _default_kwargs = {}
    
    def __init__(self, pipeline):
        self.pipeline = pipeline
                        
    @property
    def default_kwargs(self):
        return self._default_kwargs
        
    def id_str(self, filt=None):
        s = self.id_str_fmt.format(self.pipeline)
        return s
    
    def jobname(self, filt=None):
        s = '{0[ticket]}-{0[field]}-{1}'.format(self.pipeline, self.name)
        if filt is not None:
            s += '-{}'.format(filt)
        return s
    
    def cmd_str(self, filt=None, **kwargs):
        cmd = '{0}.py {1[data_root]} --rerun {1.rerun} '.format(self.name, self.pipeline) 
        cmd += '--job {0} --id {1} '.format(self.jobname(filt), self.id_str(filt))
        
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
        
        return cmd
    
    def submit_cmd(self, filt=None):
        """
        """
        cmd = self.cmd_str(filt)
        return cmd
                
    def _add_dependency(self, cmd, ids):
        raise NotImplementedError
        
    def _get_dependent_jobids(self, filt=None):
        job_ids = self.pipeline.job_ids        
        id_depends = []
        if hasattr(self, 'depends'):
            for key,i in job_ids.items():
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
        
    def submit_job(self, filt=None, test=False):
        """Submits job; returns jobid
        """
        cmd = self.submit_cmd(filt)

        # Make sure all dependencies are satisfied
        id_depends = self._get_dependent_jobids(filt=filt)
        
        cmd = self._add_dependency(cmd, id_depends)
                                    
        if test:
            jobid = np.random.randint(10000)
            cmd = cmd.replace('"', "'")
            print('command submitted: {0} (jobid={1})'.format(cmd, jobid))
            cmd = 'echo "{0} batch job {1}"'.format(cmd, jobid)
            
        output = subprocess.check_output(cmd, shell=True)
        m = re.search('batch job (\d+)', output)
        if not m:
            raise RuntimeError('Cannot find job id: {0}'.format(output))
        jobid = int(m.group(1))
        return jobid

    
class BatchStage(PipelineStage):
    _default_kwargs = {'mpiexec':'-bind-to socket', 
                       'batch-type':'slurm'}
    
    def _add_dependency(self, cmd, ids):
        if len(ids) > 0:
            cmd += "--batch-submit '--afterok"
            for i in ids:
                cmd += ":{0}".format(i)
            cmd += "' "

        return cmd    
    
class ManualBatchStage(PipelineStage):
    batch_type = 'slurm'
    _default_time = 1200
    _override_batch_options = {}
    
    def write_batch_script(self, filt=None):
        jobname = self.jobname(filt)
        batchdir = self.batch_type
        if not os.path.exists(batchdir):
            os.makedirs(batchdir)
        filename = os.path.join(batchdir, '{0}.sh'.format(jobname))
        
        try:
            time = self.pipeline['kwargs'][self.name]['time'] / 60 # slurm takes minutes
        except KeyError:
            time = self._default_time / 60
        
        batch_options = {
            'job-name':jobname,
            'output':'{0}/{1}.log'.format(batchdir, jobname),
            'time':time,
            'nodes':1,
            'ntasks-per-node':self.pipeline['cores_per_node'],
            'mem':48000
        }
        batch_options = dict(batch_options, **self._override_batch_options)
        
        with open(filename, 'w') as fout:
            
            fout.write('#!/bin/bash\n')
            
            for opts in batch_options.items():
                fout.write('#SBATCH --{0}={1}\n'.format(*opts))
        
            fout.write('\n')
            fout.write('{0}\n'.format(self.cmd_str(filt=filt)))
            
        return filename
    
    def submit_cmd(self, filt=None):
        batchfile = self.write_batch_script(filt)
        cmd = 'sbatch {0} '.format(batchfile)
        return cmd            
        
    def _add_dependency(self, cmd, ids):
        if len(ids) > 0:
            cmd += '--dependency=afterok'
            for i in ids:
                cmd += ':{0}'.format(i)
        return cmd
        
class SingleFilterStage(PipelineStage):
    single_filter = True

    def id_str(self, filt=None, visits=True):
        if filt is None:
            raise ValueError('Must provide filter for {}.'.format(self.name))
        s = super(SingleFilterStage, self).id_str()
        s += 'filter={0} '.format(filt)
        if visits:
            s += 'visit={0} '.format(self.pipeline['visit'][filt])
        return s
    
class SingleFrameDriverStage(SingleFilterStage, BatchStage):
    name = 'singleFrameDriver'
    id_str_fmt = 'ccd={0[ccd]} '

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
    
    def cmd_str(self, filt=None):
        skymap_file = self.skymap
        if skymap_file is None:
            raise ValueError("No precomputed skymap provided!  " +
                             "Need to implement actual skymap generation to continue.")
        
        target_dir = '{0.pipeline.rerun_dir}/deepCoadd/'.format(self)
        cmd = 'mkdir -p {0}; ln -s {1.skymap} {0} '.format(target_dir, self)
        return cmd
    
class MosaicStage(SingleFilterStage, ManualBatchStage):
    name = 'mosaic'
    id_str_fmt = 'tract={0[tract]} ccd={0[ccd]} '
    depends = ('singleFrameDriver', 'makeDiscreteSkyMap')
                            
class CoaddDriverStage(SingleFilterStage, BatchStage):
    name = 'coaddDriver'
    id_str_fmt = 'tract={0[tract]} '
    depends = ('singleFrameDriver', 'makeDiscreteSkyMap', 'mosaic')
            
    def id_str(self, filt=None):
        s = super(CoaddDriverStage, self).id_str(filt, visits=False)
        return s
        
    def selectId_str(self, filt=None):
        s = 'ccd={0[ccd]} '.format(self.pipeline)
        s += 'filter={0} '.format(filt)
        s += 'visits={0} '.format(self.pipeline['visit'][filt])
        return s
    
class MultiBandDriverStage(BatchStage):
    name = 'multiBandDriver'
    depends = ('singleFrameDriver', 'makeDiscreteSkyMap', 'mosaic', 'coaddDriver')
    
    def id_str(self, filt=None):
        s = 'tract={0[tract]} '.format(self.pipeline)
        all_filters = '^'.join(self.pipeline.filters)
        s += 'filter={0} '.format(all_filters)
        return s
    
class Pipeline(object):
    def __init__(self, filename):
        self.filename = filename
        self._read_yaml()
        self.job_ids = {}
        
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
            return '{0[user]}/{0[ticket]}/{0[field]}'.format(self)
            
    @property
    def rerun_dir(self):
        return '{0[data_root]}/rerun/{0.rerun}'.format(self)
            
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
    
    def run(self, test=False):
        self.job_ids = {}
        for stage in self.stages:
            if stage.single_filter:
                for filt in self.filters:
                    self.job_ids['{0}-{1}'.format(stage.name, filt)] = stage.submit_job(filt, test=test)
            else:
                self.job_ids[stage.name] = stage.submit_job(test=test)

