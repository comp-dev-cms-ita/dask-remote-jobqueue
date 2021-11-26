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
from multiprocessing import Process, Queue
from random import randrange
from re import I
from subprocess import STDOUT, check_output
from time import sleep
from typing import Optional, Union

import asyncssh
import httpx
import requests
from distributed.deploy.spec import NoOpAwaitable, SpecCluster
from distributed.deploy.ssh import Scheduler as SSHSched
from distributed.security import Security
from jinja2 import Environment, PackageLoader, select_autoescape
from loguru import logger


class ConnectionLoop(Process):

    """Class to control the tunneling processes."""

    def __init__(
        self,
        queue: "Queue",
        ssh_url: str = "",
        ssh_url_port: int = -1,
        username: str = "",
        token: str = "",
        sched_port: int = -1,
        dash_port: int = -1,
        tornado_port: int = -1,
    ):
        logger.debug(f"[ConnectionLoop][init][{ssh_url}][{ssh_url_port}]")
        super().__init__()
        self.cur_loop: "asyncio.AbstractEventLoop" = asyncio.new_event_loop()
        asyncio.set_event_loop(self.cur_loop)
        # Ref: https://asyncssh.readthedocs.io/
        self.connection = None
        self.ssh_url: str = ssh_url
        self.ssh_url_port: int = ssh_url_port
        self.username: str = username
        self.token: str = token
        self.sched_port: int = sched_port
        self.dash_port: int = dash_port
        self.tornado_port: int = tornado_port
        self.tasks: list = []
        self.queue: "Queue" = queue

    def stop(self):
        self.loop = asyncio.get_running_loop()

        async def _close_connection(connection):
            logger.debug(f"[ConnectionLoop][close connection {connection}]")
            connection.close()

        self.loop.create_task(_close_connection(self.connection))

    def run(self):
        async def forward_connection():
            logger.debug(
                f"[ConnectionLoop][connect][{self.ssh_url}][{self.ssh_url_port}][{self.token}]"
            )
            # Ref: https://asyncssh.readthedocs.io/
            self.connection = await asyncssh.connect(
                host=self.ssh_url,
                port=self.ssh_url_port,
                username=self.username,
                password=self.token,
                known_hosts=None,
            )
            logger.debug(f"[ConnectionLoop][connect][scheduler][{self.sched_port}]")
            sched_conn = await self.connection.forward_local_port(
                "127.0.0.1",
                self.sched_port,
                "127.0.0.1",
                self.sched_port,
            )

            logger.debug(f"[ConnectionLoop][connect][dashboard][{self.dash_port}]")
            dash_port = await self.connection.forward_local_port(
                "127.0.0.1", self.dash_port, "127.0.0.1", self.dash_port
            )

            logger.debug(f"[ConnectionLoop][connect][tornado][{self.tornado_port}]")
            tornado_port = await self.connection.forward_local_port(
                "127.0.0.1",
                self.tornado_port,
                "127.0.0.1",
                self.tornado_port,
            )

            if self.queue:
                self.queue.put("OK")

            await sched_conn.wait_closed()
            logger.debug(f"[ConnectionLoop][closed][scheduler][{self.sched_port}]")
            await dash_port.wait_closed()
            logger.debug(f"[ConnectionLoop][closed][dashboard][{self.dash_port}]")
            await tornado_port.wait_closed()
            logger.debug(f"[ConnectionLoop][closed][tornado][{self.tornado_port}]")

            await self.connection.wait_closed()

        async def _main_loop():
            running: bool = True
            while running:
                await asyncio.sleep(6.0)
                logger.debug(f"[ConnectionLoop][running: {running}]")
                if not self.queue.empty():
                    res = self.queue.get_nowait()
                    logger.debug(f"[ConnectionLoop][Queue][res: {res}]")
                    if res and res == "STOP":
                        self.stop()
                        running = False
                        logger.debug("[ConnectionLoop][Exiting in ... 6]")
                        for i in reversed(range(6)):
                            logger.debug(f"[ConnectionLoop][Exiting in ... {i}]")
                            await asyncio.sleep(1)
            logger.debug("[ConnectionLoop][DONE]")

        logger.debug("[ConnectionLoop][create task]")
        self.tasks.append(self.cur_loop.create_task(forward_connection()))
        logger.debug("[ConnectionLoop][run main loop until complete]")
        self.cur_loop.run_until_complete(_main_loop())
        logger.debug("[ConnectionLoop][exit]")


class RemoteHTCondor(object):

    """Class to manage a dask scheduler inside an HTCondor Cluster"""

    def __init__(
        self,
        ssh_namespace="default",
        user: str = "NONE",
        ssh_url_port: int = 8122,
        asynchronous: bool = True,  # Set by dask-labextension but not used in this class
        sitename: str = "",
    ):

        try:
            logger.add("/var/log/RemoteHTCondor.log", rotation="32 MB")
        except PermissionError:
            logger.add("/tmp/log/RemoteHTCondor.log", rotation="32 MB")

        logger.debug("[RemoteHTCondor][init]")

        # Inner class status
        self.status: int = 0
        self.asynchronous: bool = asynchronous
        self.connection_q: "Queue" = Queue()
        self.connection_process: "ConnectionLoop" = ConnectionLoop(self.connection_q)

        # Address of the dask scheduler and its dashboard
        self.address: str = ""
        self.dashboard_address: str = ""

        # Tunnel ports
        self.sched_port: int = randrange(20000, 40000)
        self.dash_port: int = randrange(20000, 40000)
        self.tornado_port: int = randrange(20000, 40000)

        logger.debug(f"generated -> sched_port: {self.sched_port}")
        logger.debug(f"generated -> dash_port: {self.dash_port}")
        logger.debug(f"generated -> tornado_port: {self.tornado_port}")

        # Custom ssh port for the tunnel
        self.ssh_url_port: int = ssh_url_port

        self.cluster_id: str = ""
        self.sitename: str = sitename

        # Jupyter env vars
        self.name = (
            os.environ.get("JUPYTERHUB_USER", user) + f"-{self.sched_port}.dask-ssh"
        )
        self.dash_hostname = (
            os.environ.get("JUPYTERHUB_USER", user) + f"-{self.dash_port}.dash.dask-ssh"
        )

        self.sshNamespace = os.environ.get("SSH_NAMESPACE", ssh_namespace)

        # HTCondor vars
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

        # IAM vars
        self.token: str = os.environ.get("JUPYTERHUB_API_TOKEN", "TOKEN")
        self.refresh_token: str = os.environ.get("REFRESH_TOKEN", "REFRESHTOKEN")
        self.iam_server = os.environ.get("IAM_SERVER")
        self.client_id = os.environ.get("IAM_CLIENT_ID")
        self.client_secret = os.environ.get("IAM_CLIENT_SECRET")

        ##
        # Dask labextension variables
        #
        # scheduler_info expected struct: {
        #     'type': str -> 'Scheduler',
        #     'id': str -> 'Scheduler-e196ea92-25e3-4dab-84b0-32718a03fefc',
        #     'address': str -> 'tcp://172.17.0.2:36959',
        #     'services': dict ->{
        #          'dashboard': int -> 8787
        #     },
        #     'started': float -> 1637317001.9844875,
        #     'workers': {
        #         '0': {
        #             'nthreads': int -> 1,
        #             'memory_limit': int -> 1000000000 (1G),
        #             'id': str ->'HTCondorCluster-1',
        #             'host': str -> '172.17.0.2',
        #             'resources': dict -> {},
        #             'local_directory': str -> '/home/submituser/dask-worker-space/worker-xax404el',
        #             'name': str -> 'HTCondorCluster-1',
        #             'services': dict -> {
        #                 'dashboard': int -> 40487
        #              },
        #             'nanny': str -> 'tcp://172.17.0.2:38661'
        #         }
        #        ...
        #     }
        # }
        self._scheduler_info: dict = {"workers": {}}
        self.scheduler_address: str = ""
        self.dashboard_link: str = ""

    @property
    def logs_port(self) -> int:
        return self.tornado_port

    def __await__(self):
        """Make the class awaitable.

        Necessary for the dask-labextension utility. Here we just make
        sure that the scheduler is started.
        """

        async def closure():
            logger.debug(
                f"[RemoteHTCondor][__await__][closure IN][status: {self.status}]"
            )
            if self.status == 0:
                await self._start()
            logger.debug(
                f"[RemoteHTCondor][__await__][closure EXIT][status: {self.status}]"
            )
            return self

        return closure().__await__()

    async def __aenter__(self):
        """Enable entering in the async context."""
        logger.debug(f"[RemoteHTCondor][__aenter__][status: {self.status}]")
        await self
        assert self.status == 2
        return self

    async def __aexit__(self, typ, value, traceback):
        """Enable exiting from the async context."""
        logger.debug(f"[RemoteHTCondor][__aexit__][status: {self.status}]")
        if self.status == 2:
            f = self.close()
            if isawaitable(f):
                await f

    @property
    def scheduler_info(self) -> dict:
        logger.debug(
            f"[Scheduler][scheduler_info][scheduler_address: {self.scheduler_address}][status: {self.status}]"
        )
        if not self.scheduler_address or self.status in [0, 1]:
            return self._scheduler_info

        # Check controller
        target_url = f"http://127.0.0.1:{self.tornado_port}/"
        logger.debug(f"[Scheduler][scheduler_info][controller][url: {target_url}]")

        resp = requests.get(target_url)
        logger.debug(
            f"[Scheduler][scheduler_info][controller][check: {resp.status_code}]"
        )
        if resp.status_code != 200:
            return self._scheduler_info

        self._scheduler_info = {
            "type": "Scheduler",
            "id": None,
            "address": f"tcp://127.0.0.1:{self.sched_port}",
            "services": {"dashboard": self.dash_port},
            "workers": {},
        }

        # Get scheduler ID
        target_url = f"http://127.0.0.1:{self.tornado_port}/schedulerID"
        logger.debug(f"[Scheduler][scheduler_info][url: {target_url}]")

        resp = requests.get(target_url)
        logger.debug(
            f"[Scheduler][scheduler_info][resp({resp.status_code}): {resp.text}]"
        )
        self._scheduler_info["id"] = resp.text

        # Update the worker specs
        target_url = f"http://127.0.0.1:{self.tornado_port}/workerSpec"
        logger.debug(f"[Scheduler][scheduler_info][url: {target_url}]")

        resp = requests.get(target_url)
        logger.debug(
            f"[Scheduler][scheduler_info][resp({resp.status_code}): {resp.text}]"
        )
        self._scheduler_info["workers"] = json.loads(resp.text)

        return self._scheduler_info

    def start(self):
        if self.asynchronous:
            return self._start()
        else:
            cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
            cur_loop.run_until_complete(self._start())

    @logger.catch
    async def _start(self):
        """Start the dask cluster scheduler.

        The dask cluster scheduler will be launched as a HTCondor Job. Then, it
        will be enstablished a tunnel to communicate with it. Thus, the result
        scheduler job will be like a long running service.
        """
        if self.status == 0:
            self.status = 1
            # Prepare HTCondor Job
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
                    "job_rm.sh",
                ]

                selected_sitename = "# requirements: Nil"
                if self.sitename:
                    selected_sitename = (
                        f'requirements = ( SiteName == "{self.sitename}" )'
                    )

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
                            selected_sitename=selected_sitename,
                        )
                        logger.debug(f"[_start][{dest.name}]")
                        logger.debug(f"[_start][\n{render}\n]")
                        # print(render)
                        dest.write(render)

                cmd = "cd {}; condor_submit -spool scheduler.sub".format(tmpdirname)

                # Submit HTCondor Job to start the scheduler
                try:
                    logger.debug(f"[_start][{cmd}]")
                    cmd_out = check_output(
                        cmd, stderr=STDOUT, shell=True, env=os.environ
                    )
                    logger.debug(f"[_start][{cmd_out.decode('ascii')}]")
                except Exception as ex:
                    raise ex

                try:
                    self.cluster_id = str(cmd_out).split("cluster ")[1].strip(".\\n'")
                    logger.debug(f"[_start][{self.cluster_id}]")
                except Exception:
                    ex = Exception("Failed to submit job for scheduler: %s" % cmd_out)
                    raise ex

                if not self.cluster_id:
                    ex = Exception("Failed to submit job for scheduler: %s" % cmd_out)
                    raise ex

            # Wait for the job to be running
            job_status = 1

            # While job is idle or hold
            while job_status in [1, 5]:
                await asyncio.sleep(6.0)

                logger.debug("[_start][Check job status]")
                cmd = "condor_q {}.0 -json".format(self.cluster_id)
                logger.debug(f"[_start][{cmd}]")

                cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
                logger.debug(f"[_start][{cmd_out.decode('ascii')}]")

                try:
                    classAd = json.loads(cmd_out)
                    logger.debug(f"[_start][classAd: {classAd}]")
                except Exception as cur_ex:
                    logger.debug(f"[_start][{cur_ex}]")
                    ex = Exception(
                        "Failed to decode claasAd for scheduler: %s" % cmd_out
                    )
                    raise ex

                job_status = classAd[0].get("JobStatus")
                logger.debug(f"[_start][job_status: {job_status}]")
                if job_status == 1:
                    logger.debug(f"[_start][{self.cluster_id}.0 still idle]")
                    continue
                elif job_status == 5:
                    logger.debug(f"[_start][{self.cluster_id}.0 still hold]")
                    continue
                elif job_status != 2:
                    ex = Exception("Scheduler job in error {}".format(job_status))
                    raise ex

            await asyncio.sleep(2.0)
            # Prepare the ssh tunnel
            ssh_url = f"ssh-listener.{self.sshNamespace}.svc.cluster.local"

            logger.debug("[_start][Create ssh tunnel")
            logger.debug(f"[_start][url: {ssh_url}]")
            logger.debug(f"[_start][username: {self.name}]")
            logger.debug(f"[_start][password: {self.token}]")

            self.connection_process = ConnectionLoop(
                self.connection_q,
                ssh_url=ssh_url,
                ssh_url_port=self.ssh_url_port,
                username=self.name,
                token=self.token,
                sched_port=self.sched_port,
                dash_port=self.dash_port,
                tornado_port=self.tornado_port,
            )
            logger.debug("[_start][Start connection process]")
            self.connection_process.start()
            logger.debug("[_start][Wait for queue...]")
            while self.connection_q.empty():
                pass
            logger.debug("[_start][Check connection_q response]")
            started_tunnels = self.connection_q.get()
            logger.debug(f"[_start][response: {started_tunnels}]")
            if started_tunnels != "OK":
                raise Exception("Cannot make any tunnel...")

            self.address = "localhost:{}".format(self.sched_port)
            self.dashboard_address = "http://localhost:{}".format(self.dash_port)

            logger.debug(f"[_start][address: {self.address}]")
            logger.debug(f"[_start][dashboard_address: {self.dashboard_address}]")

            self.scheduler_address = self.address
            self.dashboard_link = f"{self.dashboard_address}/status"

            logger.debug(f"[_start][scheduler_address: {self.scheduler_address}]")
            logger.debug(f"[_start][dashboard_link: {self.dashboard_link}]")
            logger.debug(
                f"[_start][tornado_address: http://localhost:{self.tornado_port}]"
            )

            await asyncio.sleep(6.0)

            logger.debug("[_start][Test connections...]")
            async with httpx.AsyncClient() as client:
                target_url = f"http://localhost:{self.tornado_port}"
                logger.debug(f"[_start][check controller][{target_url}]")
                resp = await client.get(target_url)
                logger.debug(f"[_start][check controller][resp({resp.status_code})]")
                if resp.status_code != 200:
                    raise Exception("Cannot connect to controller")

                target_url = self.dashboard_link
                logger.debug(f"[_start][check dashboard][{target_url}]")
                resp = await client.get(target_url)
                logger.debug(f"[_start][check dashboard][resp({resp.status_code})]")
                if resp.status_code != 200:
                    raise Exception("Cannot connect to dashboard")

            self.status = 2

    def close(self):
        if self.asynchronous:
            return self._close()
        else:
            cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
            cur_loop.run_until_complete(self._close())

    @logger.catch
    async def _close(self):
        # Close the dask cluster
        target_url = f"http://127.0.0.1:{self.tornado_port}/close"
        logger.debug(f"[Scheduler][close][url: {target_url}]")

        async with httpx.AsyncClient() as client:
            resp = await client.get(target_url)
            logger.debug(f"[Scheduler][close][resp({resp.status_code}): {resp.text}]")

        # Remove the HTCondor dask scheduler job
        cmd = "condor_rm {}.0".format(self.cluster_id)
        logger.debug(cmd)

        try:
            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
            logger.debug(str(cmd_out))
        except Exception as ex:
            raise ex

        if str(cmd_out) != "b'Job {}.0 marked for removal\\n'".format(self.cluster_id):
            raise Exception("Failed to hold job for scheduler: %s" % cmd_out)

        await asyncio.sleep(1.0)

        # Close scheduler connection
        self.connection_q.put("STOP")

        await asyncio.sleep(1.0)

        self.status = 0
        # To avoid wrong call on scheduler_info when make_cluster after deletion
        self.scheduler_address = ""

    def scale(self, n: int):
        if self.asynchronous:
            return self._scale(n)
        else:
            cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
            cur_loop.run_until_complete(self._scale(n))

    @logger.catch
    async def _scale(self, n: int):
        # Scale the cluster
        target_url = f"http://127.0.0.1:{self.tornado_port}/jobs?num={n}"
        logger.debug(f"[Scheduler][scale][num: {n}][url: {target_url}]")

        async with httpx.AsyncClient() as client:
            resp = await client.get(target_url)
            logger.debug(f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]")

    def adapt(self, minimum: int, maximum: int):
        if self.asynchronous:
            return self._adapt(minimum, maximum)
        else:
            cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
            cur_loop.run_until_complete(self._adapt(minimum, maximum))

    @logger.catch
    async def _adapt(self, minimum: int, maximum: int):
        # adapt the cluster
        target_url = f"http://127.0.0.1:{self.tornado_port}/adapt?minimum={minimum}&maximum={maximum}"
        logger.debug(
            f"[Scheduler][adapt][minimum: {minimum}|maximum: {maximum}][url: {target_url}]"
        )

        async with httpx.AsyncClient() as client:
            resp = await client.get(target_url)
            logger.debug(f"[Scheduler][adapt][resp({resp.status_code}): {resp.text}]")
