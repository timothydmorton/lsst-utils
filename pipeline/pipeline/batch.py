import os, re
import subprocess
import time
import pandas as pd
import numpy as np
from StringIO import StringIO

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
        cmd = 'sacct -j {0} --format {1}'.format(id_str, info_str)
        o = subprocess.check_output(cmd, shell=True)

        template_df = pd.DataFrame(index=ids)

        df = pd.read_table(StringIO(o), skiprows=2, header=None, names=info, delim_whitespace=True,
                            index_col=0)
        # ensure string type just in case there's just one.
        try:
            df.index = df.index.astype('str')
        except TypeError:
            df.index = df.index.astype('object')

        df = df.join(template_df, how='outer')
        keep_indices = [i for i in df.index if re.search('^\d+$', str(i))]
        df = df.ix[keep_indices]
        df['job'] = None
        for i,j in zip(ids, jobs):
            if i not in df.index:
                print(i, df.index)
                df.ix[i] = None
            df.ix[i, 'job'] = j
        # print(df)
        # template_df.ix[df.index] = df
        # df = template_df

        results.append(df)


    return results