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

def get_job_status(jobid, wait=10):
    m = False
    repeat = 0
    while not m and repeat < wait:    
        cmd = 'sacct -b -j {0}'.format(jobid)
        output = subprocess.check_output(cmd, shell=True)
        m = re.search('{0}\s+([A-Z]+)'.format(jobid), output)
        time.sleep(1)
        repeat += 1 

    if not m:
        raise ValueError('Job not found: {0}'.format(jobid))
    return m.group(1)

