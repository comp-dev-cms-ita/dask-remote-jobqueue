# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import json
import time
import tempfile
import math
import asyncssh
import os
from random import randrange
from dask_jobqueue.htcondor import HTCondorJob
from subprocess import check_output, STDOUT
from jinja2 import Environment, PackageLoader, select_autoescape

from distributed.security import Security

from distributed.deploy.spec import ProcessInterface, SpecCluster, NoOpAwaitable


class Process(ProcessInterface):
    """A superclass for SSH Workers and Nannies
    See Also
    --------
    Scheduler
    """

    def __init__(self, **kwargs):
        self.connection = None
        super().__init__(**kwargs)

    async def start(self):
        assert self.connection
        await super().start()

    async def close(self):
        await super().close()
        self.connection.close()

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
    sched_port: int
        The port to bind for scheduler
    dashboard_port: int
        The port to bind for dask dasahboard
    """

    def __init__(self, sched_port=8989, dashboard_port=8787):
        self.cluster_id = None
        self.name = os.environ.get("JUPYTERHUB_USER") + "-{}.dask-ssh".format(
            sched_port
        )
        self.dash_hostname = os.environ.get(
            "JUPYTERHUB_USER"
        ) + "-{}.dash.dask-ssh".format(dashboard_port)
        self.sched_port = sched_port
        self.dash_port = dashboard_port

        self.token = os.environ.get("JUPYTERHUB_API_TOKEN")
        self.refresh_token = os.environ.get("REFRESH_TOKEN")
        self.iam_server = os.environ.get("IAM_SERVER")
        self.client_id = os.environ.get("IAM_CLIENT_ID")
        self.client_secret = os.environ.get("IAM_CLIENT_SECRET")
        super().__init__()

    def scale(self, n=0, memory=None, cores=None):
        raise NotImplementedError()

    def adapt(
        self,
        *args,
        minimum=0,
        maximum=math.inf,
        minimum_cores: int = None,
        maximum_cores: int = None,
        minimum_memory: str = None,
        maximum_memory: str = None,
        **kwargs,
    ):
        raise NotImplementedError()

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
                    dest.write(
                        tmpl.render(
                            name=self.name,
                            token=self.token,
                            sched_port=self.sched_port,
                            dash_port=self.dash_port,
                            refresh_token=self.refresh_token,
                            iam_server=self.iam_server,
                            client_id=self.client_id,
                            client_secret=self.client_secret,
                        )
                    )

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

        self.connection = await asyncssh.connect(
            "listener.htcondor.svc.cluster.local",
            port=8122,
            username=self.name,
            password=self.token,
            known_hosts=None,
        )
        await self.connection.forward_local_port(
            "127.0.0.1", self.sched_port, "127.0.0.1", self.sched_port
        )
        await self.connection.forward_local_port(
            "127.0.0.1", self.dash_port, "127.0.01", self.dash_port
        )

        self.address = "localhost:{}".format(self.sched_port)
        self.dashboard_address = "localhost:{}".format(self.dash_port)

        await super().start()

    async def close(self):
        from dask.distributed import Client

        client = Client(address="tcp://localhost:{}".format(self.sched_port))

        try:
            client.shutdown()
        except Exception as ex:
            raise ex

        cmd = "source ~/htc.rc; condor_rm {}.0".format(self.cluster_id)

        try:
            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
        except Exception as ex:
            raise ex

        if str(cmd_out) != "b'Job {}.0 marked for removal\\n'".format(self.cluster_id):
            raise Exception("Failed to hold job for scheduler: %s" % cmd_out)

        await super().close()


class RemoteHTCondor(SpecCluster):
    def __init__(self, asynchronous=False):
        self.sched_port = randrange(20000, 40000)
        self.dashboard_port = randrange(20000, 40000)
        sched = {
            "cls": Scheduler,
            "options": {
                "sched_port": self.sched_port,
                "dashboard_port": self.dashboard_port,
            },
        }
        super().__init__(
            scheduler=sched, asynchronous=asynchronous, workers={}, name="RemoteHTC"
        )

    def scale(self, n=0, memory=None, cores=None):
        try:
            self.scheduler.scale(n=n, memory=memory, cores=cores)
        except Exception as ex:
            raise ex

        if self.asynchronous:
            return NoOpAwaitable()

    def adapt(
        self,
        *args,
        minimum=0,
        maximum=math.inf,
        minimum_cores: int = None,
        maximum_cores: int = None,
        minimum_memory: str = None,
        maximum_memory: str = None,
        **kwargs,
    ):
        try:
            self.scheduler.adapt(
                *args,
                minimum=minimum,
                maximum=maximum,
                minimum_cores=minimum_cores,
                maximum_cores=maximum_cores,
                minimum_memory=minimum_memory,
                maximum_memory=maximum_memory,
                **kwargs,
            )
        except Exception as ex:
            raise ex
