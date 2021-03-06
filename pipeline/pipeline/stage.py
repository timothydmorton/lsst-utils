from __future__ import division, print_function
import yaml
import tempfile
import os, re
import logging
import numpy as np
import subprocess
import time
import multiprocessing

from .batch import write_slurm_script, get_job_status


class KwargDict(dict):
    def cmd_str(self, skip=["filters"]):
        if skip is None:
            skip = []
        cmd = ""
        for kw, val in self.items():
            if kw in skip:
                continue

            if kw == "config":
                # val should be a dict
                for k, v in val.items():
                    cmd += "--config {0}={1} ".format(k, v)
                continue

            # If false than this is a flag/switch argument; don't turn it on.
            if val is False:
                continue

            cmd += "--{0}".format(kw)
            if val != "" and val is not True:
                # ensure quotation marks where necessary
                if isinstance(val, basestring):
                    val = '"{0}"'.format(val)

                cmd += "={0} ".format(val)
            else:
                cmd += " "

        return cmd

    def update(self, other):
        """Just like normal update, but treat 'config' specially.
        """
        other = other.copy()
        if "config" in self and "config" in other:
            self["config"].update(other["config"])
            other["config"] = self["config"]

        super(KwargDict, self).update(other)


class PipelineStage(object):
    name = None
    id_str_fmt = None
    batch_compatible = True
    single_filter = False
    batch_type = "slurm"

    _default_kwargs = {}
    _kwarg_skip = ("total_cores",)

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self._dataIds = None

    @property
    def _id_options(self):
        raise NotImplementedError("Need to define what id keys are valid.")

    @property
    def default_kwargs(self):
        return self._default_kwargs

    def id_str(self, filt=None):
        s = ""
        for key in self._id_options:
            try:
                if key == "visit":
                    s += "visit={0} ".format(self.pipeline["visit"][filt])
                else:
                    fmt = "{0}={{0[{0}]}} ".format(key)
                    s += fmt.format(self.pipeline)
            except KeyError:
                continue

        if filt is not None:
            s += "filter={0} ".format(filt)
        return s

    def kwarg_str(self, filt=None, **kwargs):
        """Returns a string of arguments 

        example YAML:

            kwargs:
                singleFrameDriver:
                    time: 1200 # sec
                    cores: 240
                makeDiscreteSkyMap:
                    time: 300
                mosaic:
                    time: 4800
                    numCoresForRead: 24
                    diagnostics: True
                    #loglevel: debug
                coaddDriver:
                    time: 900 #more than necessary, but hopefully enough for Y-band
                    cores: 20
                    HSC-Y:
                        config:
                            assembleCoadd.subregionSize: "10000,50"
                multiBandDriver:
                    time: 18000
                    cores: 864
                all:
                    clobber-config: False

        This returns a `CmdLineTask`-compatible keyword string for this task, 
        to be added to the id string to make the whole command line call string.
        """

        kws = KwargDict(self.default_kwargs)
        kws.update(self.pipeline["kwargs"]["all"])
        if self.name in self.pipeline["kwargs"]:
            kws.update(self.pipeline["kwargs"][self.name])
            if filt in self.pipeline["kwargs"][self.name]:
                kws.update(self.pipeline["kwargs"][self.name][filt])
        kws.update(kwargs)

        skip = self._kwarg_skip + tuple(self.pipeline.filters)
        return kws.cmd_str(skip=skip)

    def jobname(self, filt=None):
        try:
            s = "{0}-{1}".format(self.pipeline["job"], self.name)
        except KeyError:
            job = self.pipeline.rerun_unique.replace("/", "-")
            s = "{0}-{1}".format(job, self.name)
        if filt is not None:
            s += "-{}".format(filt)
        return s

    def _testify_cmd_str(self, cmd):
        cmd = cmd.replace('"', "'")
        np.random.seed()
        jobid = np.random.randint(10000)
        sleep = np.random.random() * 2 + 2
        cmd = 'echo "{0}\nbatch job {1}; sleep {2}"'.format(cmd, jobid, sleep)
        return cmd

    def cmd_str(self, filt=None, test=False, **kwargs):
        cmd = "{0}.py {1[data_root]} --rerun {1.rerun} ".format(self.name, self.pipeline)

        id_str = self.id_str(filt)
        if id_str:
            cmd += "--id {0} ".format(id_str)

        if hasattr(self, "selectId_str"):
            cmd += "--selectId {0} ".format(self.selectId_str(filt))

        cmd += self.kwarg_str(filt=filt, **kwargs)

        if test:
            cmd = self._testify_cmd_str(cmd)
            print(cmd)

        return cmd

    def submit_cmd(self, filt=None, test=False, **kwargs):
        """
        """
        cmd = self.cmd_str(filt, test=test, **kwargs)
        return cmd

    def _get_dependent_jobids(self, filt=None):
        job_ids = self.pipeline.job_ids
        id_depends = []
        if hasattr(self, "depends"):
            for key, i in job_ids.items():
                # If we know it's done, then skip.
                if i in self.pipeline.complete_job_ids:
                    continue

                for d in self.depends:
                    m = re.match(d, key)
                    if m:
                        if filt is not None:
                            if key == "{0}-{1}".format(d, filt) or key == d:
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
                name += " {0}".format(filt)
            msg = "{0} waiting for jobids {1}...".format(name, id_depends)
            print(msg)

        while len(id_depends) > 0:
            completed = []
            for jid in id_depends:
                if test:
                    if np.random.random() < 0.3:
                        status = "COMPLETED"
                    else:
                        status = "RUNNING"
                else:
                    status = get_job_status(jid)

                if status in ("RUNNING", "PENDING"):
                    continue
                elif status == "COMPLETED":
                    print("jobid {0} completed. ".format(jid))
                    completed.append(jid)
                    self.pipeline.complete_job_ids.append(jid)
                elif status in ("FAILED", "CANCELLED", "TIMEOUT"):
                    time.sleep(5)
                    status = get_job_status(jid)
                    if status in ("FAILED", "CANCELLED", "TIMEOUT"):
                        raise RuntimeError("Unexpected status: Job {0} is {1}.".format(jid, status))
                    else:
                        continue

                else:
                    logging.warning("Unknown status for {0}: {1}.".format(jid, status))

            for jid in completed:
                id_depends.remove(jid)
            time.sleep(2)

    def _getDataIds(self, filt=None):
        cmd = self.cmd_str(filt)
        cmd += " --show data | grep dataId"

        lines = subprocess.check_output(cmd, shell=True).splitlines()
        return lines

    @property
    def dataIds(self):
        if self._dataIds is None:
            if self.single_filter:
                filters = self.pipeline.filters
                pool = multiprocessing.Pool(len(filters))
                worker = dataIdWorker(self)
                ids = pool.map(worker, filters)
                self._dataIds = {f: i for f, i in zip(filters, ids)}
                pool.close()
                pool.join()
            else:
                self._dataIds = {None: self.getDataIds()}

        return self._dataIds

    def filterWeights(self, filters):
        total = sum([len(self.dataIds[f]) for f in filters])
        return {f: len(self.dataIds[f]) / total for f in filters}

    def get_jobid(self, output):
        m = re.search("batch job (\d+)", output)
        if not m:
            logging.error("Cannot find job id: {0}".format(output))
            logging.error("Command submitted: {0}".format(cmd))
            raise RuntimeError("Cannot find job id.")
        jobid = int(m.group(1))
        return jobid

    def submit_job(self, filt=None, test=False, **kwargs):
        """Submits job; returns jobid
        """
        self._wait_for_dependencies(filt=filt, test=test)

        cmd = self.submit_cmd(filt, test=test, **kwargs)

        output = subprocess.check_output(cmd, shell=True)
        jobid = self.get_jobid(output)
        with open(self.pipeline.logfile, "a") as fout:
            fout.write("{0} {1}\n".format(self.jobname(filt=filt), jobid))
        return jobid


class dataIdWorker(object):
    def __init__(self, stage):
        self.stage = stage

    def __call__(self, filt):
        return self.stage._getDataIds(filt)


class ManualBatchStage(PipelineStage):
    _default_time = 1200
    _override_batch_options = {}
    _kwarg_skip = ("time",)

    def write_batch_script(self, filt=None, test=False, **kwargs):
        jobname = self.jobname(filt)
        batchdir = self.pipeline.output_dir
        if not os.path.exists(batchdir):
            os.makedirs(batchdir)
        filename = os.path.join(batchdir, "{0}.sh".format(jobname))

        try:
            time = self.pipeline["kwargs"][self.name]["time"] / 60  # slurm takes minutes
        except KeyError:
            time = self._default_time / 60

        if time < 65:
            time = 65

        batch_options = {
            "job-name": jobname,
            "output": "{0}/{1}.%j.log".format(batchdir, jobname),
            "time": int(time),
            "nodes": 1,
            "ntasks-per-node": self.pipeline["cores_per_node"],
        }
        batch_options = dict(batch_options, **self._override_batch_options)
        batch_options = dict(batch_options, **kwargs)

        write_slurm_script(filename, self.cmd_str(filt=filt, test=test), **batch_options)

        self.batchfile = filename
        self.logfile = batch_options["output"]
        return filename

    def submit_cmd(self, filt=None, test=False):
        batchfile = self.write_batch_script(filt, test=test)
        cmd = "sbatch {0} ".format(batchfile)
        return cmd


class HeadNodeStage(PipelineStage):
    def get_jobid(self, output):
        return -1


class BatchStage(PipelineStage):
    @property
    def _default_kwargs(self):
        kws = {"mpiexec": "-bind-to socket", "batch-type": self.batch_type}

        kws["batch-output"] = self.pipeline.output_dir
        return kws

    def cmd_str(self, filt=None, test=False, **kwargs):
        cmd = super(BatchStage, self).cmd_str(filt=filt, test=False, **kwargs)
        cmd += "--job {0} ".format(self.jobname(filt))

        if test:
            cmd = self._testify_cmd_str(cmd)
            print(cmd)

        return cmd


class SingleFrameDriverStage(BatchStage):
    name = "singleFrameDriver"
    id_str_fmt = "ccd={0[ccd]} "
    single_filter = True
    _id_options = ("ccd", "visit")


class MakeSkyMapStage(ManualBatchStage):
    name = "makeSkyMap"
    _override_batch_options = {"ntasks-per-node": 1}

    def id_str(self, filter=None):
        return ""


class MakeDiscreteSkyMapStage(ManualBatchStage):
    name = "makeDiscreteSkyMap"
    # depends = ('singleFrameDriver', )
    _override_batch_options = {"ntasks-per-node": 1}

    @property
    def skymap(self):
        try:
            return self.pipeline["skymap"]
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
            raise ValueError(
                "No precomputed skymap provided!  " + "Need to implement actual skymap creation to continue."
            )

        target_dir = "{0.pipeline.rerun_dir}/deepCoadd/".format(self)
        cmd = "mkdir -p {0}; ln -sf {1.skymap} {0} ".format(target_dir, self)
        return cmd


class MosaicStage(ManualBatchStage):
    name = "mosaic"
    _id_options = ("tract", "patch", "ccd")
    depends = ("singleFrameDriver", "makeDiscreteSkyMap")
    single_filter = True

    def diagDir(self, filt):
        return os.path.join(os.path.expanduser("~"), "mosaicDiag", self.pipeline.rerun, filt)

    def cmd_str(self, filt=None, test=False):
        cmd = super(MosaicStage, self).cmd_str(filt=filt, test=test)
        try:
            if self.pipeline["kwargs"]["mosaic"]["diagnostics"]:
                cmd += "--diagDir {0} ".format(self.diagDir(filt))
        except KeyError:
            pass

        return cmd


class CoaddDriverStage(BatchStage):
    name = "coaddDriver"
    _id_options = ("tract", "patch")
    depends = ("singleFrameDriver", "mosaic")
    single_filter = True

    def selectId_str(self, filt=None):
        s = "ccd={0[ccd]} ".format(self.pipeline)
        s += "filter={0} ".format(filt)
        s += "visit={0} ".format(self.pipeline["visit"][filt])
        return s


class MultiBandDriverStage(BatchStage):
    name = "multiBandDriver"
    depends = ("coaddDriver",)
    _id_options = ("tract", "patch")

    def id_str(self, filt=None):
        all_filters = "^".join(self.pipeline.filters)
        return super(MultiBandDriverStage, self).id_str(filt=all_filters)


class CoaddAnalysisStage(ManualBatchStage):
    name = "coaddAnalysis"
    depends = ("multiBandDriver",)
    _id_options = ("tract", "patch")
    single_filter = True


class VisitAnalysisStage(ManualBatchStage):
    name = "visitAnalysis"
    depends = ("multiBandDriver",)
    _id_options = ("visit",)
    single_filter = True

    def cmd_str(self, filt=None, test=False):
        cmd = super().cmd_str(filt=filt, test=test)
        cmd += "--tract {}".format(self.pipeline._dict["tract"])
        return cmd


class MatchVisitsStage(ManualBatchStage):
    name = "matchVisits"
    depends = ("coaddAnalysis", "visitAnalysis")
    _id_options = ("tract",)
    single_filter = True


class ColorAnalysisStage(ManualBatchStage):
    name = "colorAnalysis"
    depends = ("multiBandDriver",)
    _id_options = ("tract", "patch")

    def id_str(self, filt=None):
        all_filters = "^".join(self.pipeline.filters)
        return super(HscColorAnalysisStage, self).id_str(filt=all_filters)
