import yaml
import tempfile
import os, re, shutil
import logging
import numpy as np
import subprocess
import time, datetime
import multiprocessing
import socket

from pkg_resources import resource_filename

from lsst.daf.persistence import Butler

from .stage import (
    SingleFrameDriverStage,
    MakeSkyMapStage,
    MosaicStage,
    CoaddDriverStage,
    MultiBandDriverStage,
    CoaddAnalysisStage,
    VisitAnalysisStage,
    MatchVisitsStage,
    ColorAnalysisStage,
)


def read_field_config(field):
    filename = resource_filename("pipeline", os.path.join("fields", "{}.yaml".format(field)))
    with open(filename) as fin:
        d = yaml.load(fin)

    skymap_file = resource_filename(
        "pipeline", os.path.join("pipeline", "fields", "{}.skymap".format(field))
    )
    if os.path.exists(skymap_file):
        d["skymap"] = skymap_file

    return d


class Pipeline(object):
    def __init__(self, filename):
        self.filename = filename
        self._read_yaml()
        self.job_ids = {}
        self.complete_job_ids = []

        self._stages = None
        self._butler = None
        self._fields = None

    def _read_yaml(self):
        with open(self.filename) as fin:
            self._dict = yaml.load(fin)

        if "field" in self._dict:
            self._dict.update(read_field_config(self._dict["field"]))

    def __getitem__(self, item):
        for s in self.stages:
            if s.name == item:
                return s

        return self._dict[item]

    @property
    def filters(self):
        return self["visit"].keys()

    @property
    def user(self):
        if "user" in self._dict:
            return self._dict["user"]
        else:
            return os.getenv("USER")

    @property
    def rerun_base(self):
        dirname = self.user
        if re.search("lsst-dev", socket.gethostname()):
            dirname = os.path.join("private", dirname)
        return dirname

    @property
    def rerun_unique(self):
        if "rerun" in self._dict:
            return self["rerun"]
        else:
            return "{0[ticket]}/{0[field]}".format(self)

    @property
    def rerun(self):
        if "rerun" in self._dict:
            return self["rerun"]
        else:
            return "{0.rerun_base}/{0.rerun_unique}".format(self)

    @property
    def rerun_dir(self):
        return "{0[data_root]}/rerun/{0.rerun}".format(self)

    @property
    def kwargs(self):
        return self["kwargs"]

    @property
    def butler(self):
        if self._butler is None:
            self._butler = Butler(self.rerun_dir)
        return self._butler

    @property
    def skymap(self):
        return self.butler.get("deepCoadd_skyMap")

    @property
    def output_dir(self):
        m = re.search("(.*)\.ya?ml", self.filename)
        return "{0}_output".format(m.group(1))

    @property
    def stages(self):
        if self._stages is None:
            stage_types = [eval("{}Stage".format(s[0].upper() + s[1:])) for s in self._dict["pipeline"]]
            self._stages = [t(self) for t in stage_types]
        return self._stages

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

    @property
    def logfile(self):
        return os.path.join(self.output_dir, "pipe.log")

    def run(self, test=False, parallel=True, clobber=False):
        # Should test to make sure Stage executables are found.

        if not test:
            if os.path.exists(self.output_dir):
                if clobber:
                    shutil.rmtree(self.output_dir)

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

            shutil.copy(self.filename, self.output_dir)

        with open(self.logfile, "a") as fout:
            fout.write("=" * 30 + "\n")
            if test:
                fout.write("TEST\n")
            fout.write("{}\n".format(datetime.datetime.now()))
            with open(self.filename) as fin:
                fout.write(fin.read())
            fout.write("=" * 30 + "\n")

        self.job_ids = {}
        self.complete_job_ids = []
        for stage in self.stages:
            if stage.single_filter:
                try:
                    filters = self["kwargs"]["filters"]
                except KeyError:
                    filters = self.filters

                # For now, do this only for singleFrameDriver
                if stage.name == "singleFrameDriver":
                    weights = stage.filterWeights(filters)
                else:
                    weights = None

                if parallel:
                    pool = multiprocessing.Pool(len(filters))
                    worker = submitWorker(stage, test=test, weights=weights)
                    jobids = pool.map(worker, filters)
                    keys = ["{0}-{1}".format(stage.name, f) for f in filters]
                    for k, j in zip(keys, jobids):
                        self.job_ids[k] = j
                    pool.close()
                    pool.join()
                else:
                    for filt in filters:
                        jobid = stage.submit_job(filt, test=test)
                        key = "{0}-{1}".format(stage.name, filt)
                        self.job_ids[key] = jobid
                        print("{0} launched (jobid={1})".format(key, jobid))
            else:
                key = stage.name
                jobid = stage.submit_job(test=test)
                self.job_ids[key] = jobid
                print("{0} launched (jobid={1})".format(key, jobid))

    def write_script(self, filename):
        with open(filename, "w") as fout:
            fout.writelines(self.commands)


class submitWorker(object):
    def __init__(self, stage, weights=None, test=False):
        self.stage = stage
        self.test = test
        self.weights = weights

    def __call__(self, filt):
        time.sleep(np.random.random() * 5)
        kwargs = dict(test=self.test)
        if self.weights is not None:
            try:
                total_cores = self.stage.pipeline.kwargs[self.stage.name]["total_cores"]
                kwargs["cores"] = int(self.weights[filt] * total_cores)
            except KeyError:
                pass

        jobid = self.stage.submit_job(filt, **kwargs)
        print("{0} launched for {1} (jobid={2})".format(self.stage.name, filt, jobid))
        return jobid
