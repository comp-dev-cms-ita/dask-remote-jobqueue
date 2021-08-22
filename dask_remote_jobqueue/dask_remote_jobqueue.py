# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import json
import time
import tempfile
import math
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

        super().__init__(**kwargs)

    async def start(self):
        await super().start()

    async def close(self):
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
        # TODO: hostname
        self.hostname = "dciangot-asdasd.dask-ssh"
        self.dash_hostname = "dciangot-asdasd.dash.dask-ssh"
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

        self.address = "dciangot-asdasd.dask-ssh:3443"
        self.dashboard_address = "dciangot-asdasd.dash.dask-ssh:3443" 
        await super().start()

    async def close(self):
        #from dask.distributed import Client

        #client = Client(address="tcp://dciangot-asdasd.dask-ssh:8989")

        #try:
        #    client.shutdown()
        #except Exception as ex:
        #    raise ex

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
        sched = {"cls": Scheduler, "options": {}}
        security = Security(tls_ca_file='/etc/certs/ca.crt',
               tls_client_cert='/etc/certs/tls.crt',
               tls_client_key='/etc/certs/tls.key',
               require_encryption=True)
        super().__init__(
            scheduler=sched, security=security, asynchronous=asynchronous, workers={}, name="RemoteHTC"
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


def CreateRemoteHTCondor():
    sched = {"cls": Scheduler, "options": {}}  # Use local scheduler for now

    return SpecCluster({}, sched, name="SSHCluster")
