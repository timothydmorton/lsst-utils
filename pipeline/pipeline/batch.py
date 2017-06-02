import os, re
import subprocess
import time
import pandas as pd
import numpy as np
from StringIO import StringIO
import pdb

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
    try:
        output = subprocess.check_output(cmd, shell=True)
        m = re.search('JobState=(\w+)', output)
    except subprocess.CalledProcessError:
        m = False

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

def get_pipeline_status(name, info=('jobid','State','Elapsed','start','end','exitcode')):
    pipe_logfile = os.path.join('{0}_output'.format(name), 'pipe.log')

    # Split up by ====
    with open(pipe_logfile) as fin:
        file_str = fin.read()

    pattern = re.compile('='*30 + '\n' + '((\S+ \d+\n)+)')
    m = re.findall(pattern, file_str)

    run_lists = [mm[0].splitlines() for mm in m]
    results = []
    for run in run_lists:
        jobs, ids = zip(*[l.split() for l in run])
        id_str = ','.join(ids)
        info_str = ','.join(info)

        ids = np.array(ids).astype(int)

        cmd = 'sacct -j {0} --format {1}'.format(id_str, info_str)
        o = subprocess.check_output(cmd, shell=True)

        # Filter to make sure only one line per job is kept
        keep_lines = []
        for l in o.splitlines():
            x = l.split()
            if re.search('^\d+$', x[0]):
                keep_lines.append(l)

        o = '\n'.join(keep_lines)
        if not o:
            logging.warning('No slurm jobs matching {}'.format(id_str))
            continue

        df = pd.read_table(StringIO(o), header=None, names=info, delim_whitespace=True,
                            index_col=0)

        # Add jobs; this will also add null rows for jobs without slurmdb entry
        df['job'] = None
        for i,j in zip(ids, jobs):
            if i not in df.index:
                try:
                    df.ix[i] = None
                except:
                    pdb.set_trace()
            df.ix[i, 'job'] = j

        # Reorder columns to make job name first
        new_cols = ['job'] + list(df.columns[:-1])
        df = df.ix[:, new_cols]
        results.append(df)


    return results