# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import weakref
import json
import time
import tempfile
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob
from subprocess import check_output, STDOUT
from jinja2 import Environment, PackageLoader, select_autoescape

from distributed.deploy.spec import ProcessInterface, SpecCluster
from dask_jobqueue.core import Job


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


from dask_jobqueue.htcondor import HTCondorJob


class Job(HTCondorJob):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            python="source /usr/local/share/root6/bin/thisroot.sh ; /usr/bin/python3",
        )
        self.submit_command = "./sched_submit.sh"
        # self._scheduler = "tcp://90.147.75.109:8989"
        self.executable = "/bin/bash"


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

    def __init__(self):
        self.cluster_id = None
        super().__init__()

    async def start(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            env = Environment(
                loader=PackageLoader("dask_remote_jobqueue"),
                autoescape=select_autoescape(),
            )

            files = [
                "scheduler.sh",
                "scheduler.sub",
                "start_scheduler.py",
                "job_submit.sh",
            ]

            for f in files:
                tmpl = env.get_template(f)
                with open(tmpdirname + "/" + f, "w") as dest:
                    dest.write(tmpl.render())

            cmd = "source ~/htc.rc; cd {}; condor_submit -spool scheduler.sub".format(
                tmpdirname
            )

            try:
                cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
            except Exception as ex:
                raise ex

            try:
                self.cluster_id = str(cmd_out).split("cluster ")[1].strip(".\\n'")
            except:
                raise Exception("Failed to submit job for scheduler: %s" % cmd_out)

            if not self.cluster_id:
                raise Exception("Failed to submit job for scheduler: %s" % cmd_out)

        job_status = 1
        while job_status == 1:
            time.sleep(30)
            cmd = "source ~/htc.rc; condor_q {}.0 -json".format(self.cluster_id)

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
                raise Exception("Scheduler job in error {}".format(job_status))

            startd_ip_classAd = classAd[0].get("StartdIpAddr")
            if not startd_ip_classAd:
                raise Exception("Scheduler job running but no StartdIpAddr in ClassAd")

            startd_ip = startd_ip_classAd.split(":")[0].strip("<")

        import asyncssh  # import now to avoid adding to module startup time

        self.connection = await asyncssh.connect(
            "90.147.75.109", username="root", known_hosts=None
        )
        await self.connection.forward_local_port("", 8989, startd_ip, 8989)

        self.connection_dash = await asyncssh.connect(
            "90.147.75.109", username="root", known_hosts=None
        )
        await self.connection.forward_local_port("", 8787, startd_ip, 8787)

        self.address = "localhost:8989"
        await super().start()

    async def close(self):
        cmd = "source ~/htc.rc; condor_rm {}.0".format(self.cluster_id)

        try:
            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
        except Exception as ex:
            raise ex

        if str(cmd_out) != "'Job {}.0 marked for removal\\n'".format(self.cluster_id):
            raise Exception("Failed to hold job for scheduler: %s" % cmd_out)

        await super().close()


class RemoteHTCondor(SpecCluster):
    def __init__(self, asynchronous=False):
        sched = {"cls": Scheduler, "options": {}}
        super().__init__(
            scheduler=sched, asynchronous=asynchronous, workers={}, name="RemoteHTC"
        )


def CreateRemoteHTCondor():
    # workers = {
    #     0:{
    #         "cls": Job,
    #         "options": {
    #             "cores": 1,
    #             "memory": "3GB",
    #             "disk": "1GB"
    #         }
    #     }
    #     }
    sched = {"cls": Scheduler, "options": {}}  # Use local scheduler for now

    return SpecCluster({}, sched, name="SSHCluster")
