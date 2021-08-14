# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import weakref
import json
import time
import tempfile
from dask_jobqueue import HTCondorCluster
from subprocess import check_output, STDOUT
from jinja2 import Environment, PackageLoader, select_autoescape

from distributed.deploy.spec import ProcessInterface
from distributed.core import Job, Status
import htcondor


class Process(ProcessInterface):
    """A superclass for SSH Workers and Nannies
    See Also
    --------
    Scheduler
    """

    def __init__(self, **kwargs):
        self.connection = None
        self.connection_dash = None

        super().__init__(**kwargs)

    async def start(self):
        assert self.connection
        assert self.connection_dash
        await super().start()

    async def close(self):
        self.connection.close()
        self.connection_dash.close()
        await super().close()

    def __repr__(self):
        return f"<SSH {type(self).__name__}: status={self.status}>"


class Scheduler(Process):
    """A Remote Dask Scheduler controlled via HTCondor
    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    connect_options: dict
        kwargs to be passed to asyncssh connections
    remote_python: str
        Path to Python on remote node to run this scheduler.
    kwargs: dict
        These will be passed through the dask-scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(
        self, address: str, connect_options: dict, kwargs: dict, remote_python=None
    ):
        self.cluster_id = None
        super().__init__()

    async def start(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            env = Environment(
                loader=PackageLoader("dask_remote_jobqueue"),
                autoescape=select_autoescape(),
            )

            files = ["scheduler.sh", "scheduler.sub", "start_scheduler.py"]

            for f in files:
                tmpl = env.get_template(f)
                with open(f) as dest:
                    dest.write(tmpdirname + tmpl.render())

            cmd = (
                "source ~/.htc.rc; cd {tmpdirname}; condor_submit -spool scheduler.sub"
            )

            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)

            try:
                self.cluster_id = cmd_out.split("cluster ")[1].strip(".")
            except:
                raise Exception("Failed to submit job for scheduler: %s" % cmd_out)

            if not self.cluster_id:
                raise Exception("Failed to submit job for scheduler: %s" % cmd_out)

        job_status = 1
        while job_status == 1:
            time.sleep(30)
            cmd = "source ~/.htc.rc; condor_q {self.cluster_id}.0 -json"

            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)

            try:
                classAd = json.loads(cmd_out)
            except:
                raise Exception("Failed to decode claasAd for scheduler: %s" % cmd_out)

            job_status = classAd[0].get("JobStatus")
            if job_status == 1:
                # logger.info("Job {cluster_id}.0 still idle")
                continue
            elif job_status != 2:
                raise Exception("Scheduler job in error {job_status}")

            startd_ip_classAd = classAd[0].get("StartdIpAddr")
            if not startd_ip_classAd:
                raise Exception("Scheduler job running but no StartdIpAddr in ClassAd")

            startd_ip = startd_ip_classAd.split(":")[0].strip("<")

        import asyncssh  # import now to avoid adding to module startup time

        self.connection = await asyncssh.connect("90.147.75.109")
        await self.connection.forward_local_port("", 8989, startd_ip, 8989)

        self.connection_dash = await asyncssh.connect("90.147.75.109")
        await self.connection.forward_local_port("", 8787, startd_ip, 8787)

        self.address = "localhost:8989"
        await super().start()

    async def close(self):
        cmd = "source ~/.htc.rc; condor_rm {self.cluster_id}.0"

        cmd_out = check_output(cmd, stderr=STDOUT, shell=True)

        if cmd_out != "Job {self.cluster_id}.0 marked for removal":
            raise Exception(
                "Failed to hold job {self.cluster_id} for scheduler: %s" % cmd_out
            )

        await super().close()


class RemoteHTCondorCluster(HTCondorCluster):
    def __init__(
        self,
        n_workers=0,
        job_cls: Job = None,
        # Cluster keywords
        loop=None,
        security=None,
        silence_logs="error",
        name=None,
        asynchronous=False,
        # Scheduler-only keywords
        dashboard_address=None,
        host=None,
        scheduler_options=None,
        # Options for both scheduler and workers
        interface=None,
        protocol="tcp://",
        # Job keywords
        config_name=None,
        **job_kwargs,
    ):
        scheduler = {
            "cls": Scheduler,  # Use local scheduler for now
            "options": scheduler_options,
        }

        super().__init__(
            scheduler=scheduler,
        )


def CreateRemoteHTCondor():
    return RemoteHTCondorCluster(dashboard_address="localhost:8787")
