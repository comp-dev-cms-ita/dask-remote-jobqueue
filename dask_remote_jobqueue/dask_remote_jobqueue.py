# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import asyncio
import json

# import math
import os
import tempfile
from inspect import isawaitable
from random import randrange
from re import I
from subprocess import STDOUT, check_output
from typing import Union

import httpx
from distributed.deploy.spec import NoOpAwaitable, SpecCluster
from distributed.deploy.ssh import Scheduler as SSHSched
from distributed.security import Security
from jinja2 import Environment, PackageLoader, select_autoescape
from loguru import logger


class Scheduler(SSHSched):
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
        super().__init__(address="", connect_options={}, kwargs={})

    # def __repr__(self):
    #     return f"<SSH {type(self).__name__}: status={self.status}>"

    # async def scale(self, n=0, memory=None, cores=None):
    #     pass
    #     # target_url = f"localhost:{self.tornado_port}/jobs?num={n}"
    #     # logger.debug(f"[Scheduler][scale][num: {target_url}]")

    #     # async with httpx.AsyncClient() as client:
    #     #     resp = await client.get(target_url)
    #     #     logger.debug(f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]")

    # def adapt(
    #     self,
    #     *args,
    #     minimum=0,
    #     maximum=math.inf,
    #     minimum_cores: int = None,
    #     maximum_cores: int = None,
    #     minimum_memory: str = None,
    #     maximum_memory: str = None,
    #     **kwargs,
    # ):
    #     pass
    #     # raise NotImplementedError()

    @logger.catch
    async def start(self):

        await super().start()

    # @logger.catch
    # async def close(self):
    #     logger.debug(f"connect to scheduler: tcp://127.0.0.1:{self.sched_port}")
    #     client = Client(address=f"tcp://127.0.0.1:{self.sched_port}", asynchronous=True)
    #     logger.debug(f"client: {client}")

    #     try:
    #         logger.debug("client shutdown")
    #         client.shutdown()
    #     except distributed.comm.core.CommClosedError:
    #         logger.debug("client close")
    #         client.close()
    #     except Exception as ex:
    #         raise ex

    #     cmd = "condor_rm {}.0".format(self.cluster_id)
    #     logger.debug(cmd)

    #     try:
    #         cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
    #         logger.debug(str(cmd_out))
    #     except Exception as ex:
    #         raise ex

    #     if str(cmd_out) != "b'Job {}.0 marked for removal\\n'".format(self.cluster_id):
    #         raise Exception("Failed to hold job for scheduler: %s" % cmd_out)

    #     await super().close()


class RemoteHTCondor(object):
    def __init__(
        self,
        ssh_namespace="default",
        user: str = "NONE",
        ssh_url_port: int = 8122,
        asynchronous: bool = True,
    ):

        logger.add("/var/log/RemoteHTCondor.log", rotation="32 MB")

        logger.debug("[RemoteHTCondor][init]")

        self.status: int = 0

        self.address: str = ""
        self.dashboard_address: str = ""

        self.sched_port: int = randrange(20000, 40000)
        self.dash_port: int = randrange(20000, 40000)
        self.tornado_port: int = randrange(20000, 40000)

        logger.debug(f"generated -> sched_port: {self.sched_port}")
        logger.debug(f"generated -> dash_port: {self.dash_port}")
        logger.debug(f"generated -> tornado_port: {self.tornado_port}")

        self.ssh_url_port: int = ssh_url_port

        self.cluster_id: str = ""
        self.name = (
            os.environ.get("JUPYTERHUB_USER", user) + f"-{self.sched_port}.dask-ssh"
        )
        self.dash_hostname = (
            os.environ.get("JUPYTERHUB_USER", user) + f"-{self.dash_port}.dash.dask-ssh"
        )
        self.connection = None
        self.sshNamespace = os.environ.get("SSH_NAMESPACE", ssh_namespace)

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

        # dask labextension variables
        # scheduler_info expected struct: {
        #     "workers": {
        #         "0": {
        #             "nthreads": int,
        #             "memory_limit": int,
        #         }
        #        ...
        #     }
        # }
        self.scheduler_info: dict = {"workers": {}}
        self.scheduler_address: str = ""
        self.dashboard_link: str = ""

    def __await__(self):
        async def closure():
            logger.debug("await RemoteHTCondor")
            if self.status == 0:
                await self.start()
            return self

        return closure().__await__()

    async def __aenter__(self):
        await self
        assert self.status == 1
        return self

    async def __aexit__(self, typ, value, traceback):
        f = self.close()
        if isawaitable(f):
            await f

    @logger.catch
    async def start(self):
        if self.status == 0:
            import asyncssh  # import now to avoid adding to module startup time

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
                    cmd_out = check_output(
                        cmd, stderr=STDOUT, shell=True, env=os.environ
                    )
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

            # While job is idle or hold
            while job_status in [1, 5]:
                logger.debug("Check job status")
                cmd = "condor_q {}.0 -json".format(self.cluster_id)
                logger.debug(cmd)

                cmd_out = check_output(cmd, stderr=STDOUT, shell=True)

                logger.debug(cmd_out)

                try:
                    classAd = json.loads(cmd_out)
                    logger.debug(f"classAd: {classAd}")
                except Exception as cur_ex:
                    logger.debug(cur_ex)
                    ex = Exception(
                        "Failed to decode claasAd for scheduler: %s" % cmd_out
                    )
                    raise ex

                job_status = classAd[0].get("JobStatus")
                logger.debug(f"job_status: {job_status}")
                if job_status == 1:
                    logger.debug(f"Job {self.cluster_id}.0 still idle")
                    continue
                elif job_status == 5:
                    logger.debug(f"Job {self.cluster_id}.0 still hold")
                    continue
                elif job_status != 2:
                    ex = Exception("Scheduler job in error {}".format(job_status))
                    raise ex

                await asyncio.sleep(2)

            ssh_url = f"ssh-listener.{self.sshNamespace}.svc.cluster.local"

            logger.debug("Create ssh tunnel")
            logger.debug(f"url: {ssh_url}")
            logger.debug(f"username: {self.name}")
            logger.debug(f"password: {self.token}")

            self.connection = await asyncssh.connect(
                ssh_url,
                port=self.ssh_url_port,
                username=self.name,
                password=self.token,
                known_hosts=None,
            )

            async def forward_sched():
                cur_conn = await self.connection.forward_local_port(
                    "127.0.0.1", self.sched_port, "127.0.0.1", self.sched_port
                )
                cur_conn.wait_closed()

            async def forward_dash():
                cur_conn = await self.connection.forward_local_port(
                    "127.0.0.1", self.dash_port, "127.0.0.1", self.dash_port
                )
                cur_conn.wait_closed()

            async def forward_tornado():
                cur_conn = await self.connection.forward_local_port(
                    "127.0.0.1", self.tornado_port, "127.0.0.1", self.tornado_port
                )
                cur_conn.wait_closed()

            loop = asyncio.get_running_loop()
            loop.create_task(forward_sched())
            loop.create_task(forward_dash())
            loop.create_task(forward_tornado())

            logger.debug("Wait for connections...")
            await asyncio.sleep(6)

            self.address = "localhost:{}".format(self.sched_port)
            self.dashboard_address = "http://localhost:{}".format(self.dash_port)

            logger.debug(f"address: {self.address}")
            logger.debug(f"dashboard_address: {self.dashboard_address}")

            self.scheduler_address = self.address
            self.dashboard_link = f"{self.dashboard_address}/status"

            logger.debug(f"scheduler_address: {self.scheduler_address}")
            logger.debug(f"dashboard_link: {self.dashboard_link}")

            self.status = 1

            # client = Client(address=self.address, asynchronous=True)
            # client.get
            # logger.debug(f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]")

    @logger.catch
    async def close(self):
        target_url = f"http://127.0.0.1:{self.tornado_port}/close"
        logger.debug(f"[Scheduler][close][url: {target_url}]")

        async with httpx.AsyncClient() as client:
            resp = await client.get(target_url)
            logger.debug(f"[Scheduler][close][resp({resp.status_code}): {resp.text}]")

        cmd = "condor_rm {}.0".format(self.cluster_id)
        logger.debug(cmd)

        try:
            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
            logger.debug(str(cmd_out))
        except Exception as ex:
            raise ex

        if str(cmd_out) != "b'Job {}.0 marked for removal\\n'".format(self.cluster_id):
            raise Exception("Failed to hold job for scheduler: %s" % cmd_out)

    @logger.catch
    async def scale(self, n: int):
        target_url = f"http://127.0.0.1:{self.tornado_port}/jobs?num={n}"
        logger.debug(f"[Scheduler][scale][num: {n}][url: {target_url}]")

        async with httpx.AsyncClient() as client:
            resp = await client.get(target_url)
            logger.debug(f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]")

        target_url = f"http://127.0.0.1:{self.tornado_port}/workerSpec"
        logger.debug(f"[Scheduler][scale][url: {target_url}]")

        async with httpx.AsyncClient() as client:
            resp = await client.get(target_url)
            logger.debug(f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]")
            self.scheduler_info["workers"] = json.loads(resp.text)

    @logger.catch
    async def adapt(self, minimum: int, maximum: int):
        pass

    # @logger.catch
    # async def close(self):
    #     try:
    #         logger.debug("[RemoteHTCondor][close][scheduler]")
    #         await self.scheduler.close()
    #     except Exception as ex:
    #         raise ex

    #     logger.debug("[RemoteHTCondor][close]")
    #     super().close()

    #     if self.asynchronous:
    #         return NoOpAwaitable()

    # @logger.catch
    # async def scale(self, n=0, memory=None, cores=None):
    #     logger.debug("[RemoteHTCondor][scale]")
    #     await self.scheduler.scale(n, memory, cores)

    # @logger.catch
    # def adapt(
    #     self,
    #     *args,
    #     minimum=0,
    #     maximum=math.inf,
    #     minimum_cores: int = None,
    #     maximum_cores: int = None,
    #     minimum_memory: str = None,
    #     maximum_memory: str = None,
    #     **kwargs,
    # ):
    #     try:
    #         logger.debug("[RemoteHTCondor][adapt][scheduler]")
    #         self.scheduler.adapt(
    #             *args,
    #             minimum=minimum,
    #             maximum=maximum,
    #             minimum_cores=minimum_cores,
    #             maximum_cores=maximum_cores,
    #             minimum_memory=minimum_memory,
    #             maximum_memory=maximum_memory,
    #             **kwargs,
    #         )
    #     except Exception as ex:
    #         raise ex
