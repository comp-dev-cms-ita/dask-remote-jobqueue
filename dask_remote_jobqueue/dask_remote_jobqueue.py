# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import json
import math
import os
import tempfile
import time
from random import randrange
from re import I
from subprocess import STDOUT, check_output
from typing import Union

import asyncssh
from dask import distributed
from dask.distributed import Client
from distributed.deploy.spec import NoOpAwaitable, ProcessInterface, SpecCluster
from distributed.security import Security
from jinja2 import Environment, PackageLoader, select_autoescape
from loguru import logger


class Scheduler(ProcessInterface):
    """A Remote Dask Scheduler controlled via HTCondor
    Parameters
    ----------
    sched_port: int
        The port to bind for scheduler
    dashboard_port: int
        The port to bind for dask dasahboard
    tornado_port: int
        The port to bind for tornado web app
    """

    def __init__(
        self,
        sched_port: int = 8989,
        dashboard_port: int = 8787,
        tornado_port: int = 8181,
        ssh_namespace="default",
    ):
        super().__init__()

        self.cluster_id = None
        self.name = os.environ.get("JUPYTERHUB_USER") + "-{}.dask-ssh".format(
            sched_port
        )
        self.dash_hostname = os.environ.get(
            "JUPYTERHUB_USER", "None"
        ) + "-{}.dash.dask-ssh".format(dashboard_port)
        self.connection = None
        self.sched_port: int = sched_port
        self.dash_port: int = dashboard_port
        self.tornado_port: int = tornado_port
        self.sshNamespace = ssh_namespace

        self.htc_ca = "$PWD/ca.crt"
        # os.environ.get("_condor_AUTH_SSL_CLIENT_CAFILE")
        self.htc_debug = os.environ.get("_condor_TOOL_DEBUG")
        self.htc_collector = os.environ.get("_condor_COLLECTOR_HOST")
        self.htc_schedd_host = os.environ.get("_condor_SCHEDD_HOST")
        self.htc_schedd_name = os.environ.get("_condor_SCHEDD_NAME")
        self.htc_scitoken_file = "$PWD/token"
        # os.environ.get("_condor_SCITOKENS_FILE")
        self.htc_sec_method = os.environ.get(
            "_condor_SEC_DEFAULT_AUTHENTICATION_METHODS"
        )

        self.token = os.environ.get("JUPYTERHUB_API_TOKEN")
        self.refresh_token = os.environ.get("REFRESH_TOKEN")
        self.iam_server = os.environ.get("IAM_SERVER")
        self.client_id = os.environ.get("IAM_CLIENT_ID")
        self.client_secret = os.environ.get("IAM_CLIENT_SECRET")

    def __repr__(self):
        return f"<SSH {type(self).__name__}: status={self.status}>"

    def scale(self, n=0, memory=None, cores=None):
        pass
        # raise NotImplementedError()

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
        pass
        # raise NotImplementedError()

    @logger.catch
    async def start(self):

        await super().start()

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
                    render = tmpl.render(
                        name=self.name,
                        token=self.token,
                        sched_port=self.sched_port,
                        dash_port=self.dash_port,
                        tornado_port=self.tornado_port,
                        refresh_token=self.refresh_token,
                        iam_server=self.iam_server,
                        client_id=self.client_id,
                        client_secret=self.client_secret,
                        htc_ca=self.htc_ca,
                        htc_debug=self.htc_debug,
                        htc_collector=self.htc_collector,
                        htc_schedd_host=self.htc_schedd_host,
                        htc_schedd_name=self.htc_schedd_name,
                        htc_scitoken_file=self.htc_scitoken_file,
                        htc_sec_method=self.htc_sec_method,
                    )
                    logger.debug(dest.name)
                    logger.debug(render)
                    # print(render)
                    dest.write(render)

            cmd = "cd {}; condor_submit -spool scheduler.sub".format(tmpdirname)

            try:
                logger.debug(cmd)
                cmd_out = check_output(cmd, stderr=STDOUT, shell=True, env=os.environ)
            except Exception as ex:
                raise ex

            logger.debug(str(cmd_out))

            try:
                self.cluster_id = str(cmd_out).split("cluster ")[1].strip(".\\n'")
                logger.debug(self.cluster_id)
            except Exception:
                ex = Exception("Failed to submit job for scheduler: %s" % cmd_out)
                raise ex

            if not self.cluster_id:
                ex = Exception("Failed to submit job for scheduler: %s" % cmd_out)
                raise ex

        job_status = 1
        while job_status == 1:
            logger.debug(f"Check job status")
            cmd = "condor_q {}.0 -json".format(self.cluster_id)
            logger.debug(cmd)

            time.sleep(6)
            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)

            logger.debug(cmd_out)

            try:
                classAd = json.loads(cmd_out)
                logger.debug(f"classAd: {classAd}")
            except Exception as cur_ex:
                logger.debug(cur_ex)
                ex = Exception("Failed to decode claasAd for scheduler: %s" % cmd_out)
                raise ex

            job_status = classAd[0].get("JobStatus")
            logger.debug(f"job_status: {job_status}")
            if job_status == 1:
                logger.debug(f"Job {self.cluster_id}.0 still idle")
                continue
            elif job_status != 2:
                ex = Exception("Scheduler job in error {}".format(job_status))
                raise ex

        ssh_url = f"ssh-listener.{self.sshNamespace}.svc.cluster.local"

        logger.debug("Create ssh tunnel")
        logger.debug(f"url: {ssh_url}")
        logger.debug(f"username: {self.name}")
        logger.debug(f"password: {self.token}")

        self.connection = await asyncssh.connect(
            ssh_url,
            port=8122,
            username=self.name,
            password=self.token,
            known_hosts=None,
        )
        await self.connection.forward_local_port(
            "127.0.0.1", self.sched_port, "127.0.0.1", self.sched_port
        )
        await self.connection.forward_local_port(
            "127.0.0.1", self.dash_port, "127.0.0.1", self.dash_port
        )

        logger.debug("Wait for connections...")
        time.sleep(16)

        self.address = "localhost:{}".format(self.sched_port)
        self.dashboard_address = "localhost:{}".format(self.dash_port)

        logger.debug(f"address: {self.address}")
        logger.debug(f"dashboard_address: {self.dashboard_address}")

    @logger.catch
    async def close(self):
        logger.debug(f"connect to scheduler: tcp://127.0.0.1:{self.sched_port}")
        client = Client(address=f"tcp://127.0.0.1:{self.sched_port}", asynchronous=True)
        logger.debug(f"client: {client}")

        try:
            logger.debug("client shutdown")
            client.shutdown()
        except distributed.comm.core.CommClosedError:
            logger.debug("client close")
            client.close()
        except Exception as ex:
            raise ex

        cmd = "condor_rm {}.0".format(self.cluster_id)
        logger.debug(cmd)

        try:
            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
            logger.debug(str(cmd_out))
        except Exception as ex:
            raise ex

        if str(cmd_out) != "b'Job {}.0 marked for removal\\n'".format(self.cluster_id):
            raise Exception("Failed to hold job for scheduler: %s" % cmd_out)

        await super().close()


class RemoteHTCondor(SpecCluster):
    def __init__(self, asynchronous=False, ssh_namespace="default"):

        logger.add("/var/log/RemoteHTCondor.log", rotation="32 MB")

        logger.debug("[RemoteHTCondor][init]")

        if os.environ.get("SSH_NAMESPACE"):
            ssh_namespace = os.environ.get("SSH_NAMESPACE")

        self.sched_port = randrange(20000, 40000)
        self.dashboard_port = randrange(20000, 40000)
        self.tornado_port = randrange(20000, 40000)
        self.scheduler: Union["Scheduler", None] = None

        sched = {
            "cls": Scheduler,
            "options": {
                "sched_port": self.sched_port,
                "dashboard_port": self.dashboard_port,
                "tornado_port": self.tornado_port,
                "ssh_namespace": ssh_namespace,
            },
        }
        super().__init__(
            name="RemoteHTC", scheduler=sched, asynchronous=asynchronous, workers={}
        )

    @logger.catch
    async def close(self):
        try:
            logger.debug("[RemoteHTCondor][close][scheduler]")
            await self.scheduler.close()
        except Exception as ex:
            raise ex

        logger.debug("[RemoteHTCondor][close]")
        super().close()

        if self.asynchronous:
            return NoOpAwaitable()

    @logger.catch
    def scale(self, n=0, memory=None, cores=None):
        try:
            logger.debug("[RemoteHTCondor][scale][scheduler]")
            self.scheduler.scale(n=n, memory=memory, cores=cores)
        except Exception as ex:
            raise ex

        if self.asynchronous:
            return NoOpAwaitable()

    @logger.catch
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
            logger.debug("[RemoteHTCondor][adapt][scheduler]")
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
