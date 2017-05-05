import os, re
import subprocess
import time

def write_slurm_script(filename, cmd, **batch_options):
    with open(filename, 'w') as fout:
        fout.write('#!/bin/bash\n')
        for opts in batch_options.items():
            fout.write('#SBATCH --{0}={1}\n'.format(*opts))
        
        fout.write('\n')
        fout.write('{0}\n'.format(cmd))

def get_job_status(jobid, wait=30):
    """Returns status of slurm job <jobid>

    Currently parses output of `sacct`.  Perhaps would
    be a good idea to move this to pyslurm (though this would 
    add a dependency.)
    """
    cmd = 'scontrol show job {0}'.format(jobid)
    output = subprocess.check_output(cmd, shell=True)
    m = re.search('JobState=(\w+)', output)
    status = None
    if m:
        status = m.group(1)
    else:
        repeat = 0
        while not m and repeat < wait:    
            cmd = 'sacct -b -j {0}'.format(jobid)
            output = subprocess.check_output(cmd, shell=True)
            m = re.search('{0}\s+([A-Z]+)'.format(jobid), output)
            time.sleep(1)
            repeat += 1 
        if m:
            status = m.group(1)   

    if status is None:
        raise ValueError('Job not found: {0}'.format(jobid))
    else:
        return status

def get_pipeline_status(name):
    pipe_logfile = os.path.join('{0}_output'.format(name), 'pipe.log')

    # Split up by ====
    with open(pipe_logfile) as fin:
        file_str = fin.read()

    pattern = re.compile('='*30 + '\n' + '(20\d\d-\d\d-\d\d.*)\n' + '.*' + '='*30 + '\n' + '(.*)' + '($|=)')
    m = re.findall(pattern, file_str)
    return pattern, m