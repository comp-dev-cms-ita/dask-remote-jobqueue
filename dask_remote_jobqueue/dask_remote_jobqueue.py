# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import asyncio
import json

# import math
import os
import weakref
from dataclasses import dataclass
from enum import Enum
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
from loguru import logger

from .utils import ConnectionLoop, StartDaskScheduler


class State(Enum):
    idle = 1
    start = 2
    scheduler_up = 3
    waiting_connections = 4
    running = 5


@dataclass
class AdaptiveProp:
    """Class for keeping track of minimum and maximum workers in adaptive mode."""

    minimum: int
    maximum: int


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
        self.state: State = State.idle
        self.asynchronous: bool = asynchronous

        self.connection_process_q: "Queue" = Queue()
        self.connection_process: "ConnectionLoop" = ConnectionLoop(
            self.connection_process_q
        )
        self.start_sched_process_q: "Queue" = Queue()
        self.start_sched_process: "StartDaskScheduler" = StartDaskScheduler(
            weakref.proxy(self), self.start_sched_process_q, os.environ
        )

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
        self.htc_ca = "./ca.crt"
        # os.environ.get("_condor_AUTH_SSL_CLIENT_CAFILE")
        self.htc_debug = os.environ.get("_condor_TOOL_DEBUG")
        self.htc_collector = os.environ.get("_condor_COLLECTOR_HOST")
        self.htc_schedd_host = os.environ.get("_condor_SCHEDD_HOST")
        self.htc_schedd_name = os.environ.get("_condor_SCHEDD_NAME")
        self.htc_scitoken_file = "./token"
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

    async def __aenter__(self):
        """Enable entering in the async context."""
        logger.debug(f"[RemoteHTCondor][__aenter__][state: {self.state}]")
        await self
        assert self.state == State.running
        return self

    def __await__(self):
        """Make the class awaitable.

        Necessary for the dask-labextension utility. Here we just make
        sure that the scheduler is started.
        """

        async def closure():
            logger.debug(
                f"[RemoteHTCondor][__await__][closure IN][state: {self.state}]"
            )
            if self.state == State.idle:
                await self._start()
            logger.debug(
                f"[RemoteHTCondor][__await__][closure EXIT][state: {self.state}]"
            )
            return self

        return closure().__await__()

    async def __aexit__(self, typ, value, traceback):
        """Enable exiting from the async context."""
        logger.debug(f"[RemoteHTCondor][__aexit__][state: {self.state}]")
        if self.state == State.running:
            f = self.close()
            if isawaitable(f):
                await f

    @property
    def scheduler_info(self) -> dict:
        logger.debug(
            f"[Scheduler][scheduler_info][scheduler_address: {self.scheduler_address}][state: {self.state}]"
        )
        if self.state != State.running:
            if self.asynchronous:
                if self.state == State.start:
                    logger.debug(
                        "[Scheduler][scheduler_info][waiting for scheduler update...]"
                    )
                    if not self.start_sched_process_q.empty():
                        msg = self.start_sched_process_q.get()
                        if msg == "SCHEDULERJOB==IDLE":
                            self.scheduler_address = "Job is idle..."
                        elif msg == "SCHEDULERJOB==HOLD":
                            self.scheduler_address = "Job is hold..."
                        elif msg == "SCHEDULERJOB==RUNNING":
                            self.state = State.scheduler_up
                            self.scheduler_address = "Waiting for connection..."

                elif self.state == State.scheduler_up:
                    logger.debug("[Scheduler][scheduler_info][make connections...]")
                    cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
                    cur_loop.create_task(self._make_connections())
                    self.state = State.waiting_connections

                return self._scheduler_info
            else:
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
            self.start_sched_process.join()
            cur_loop.run_until_complete(self._make_connections())

    @logger.catch
    async def _start(self):
        """Start the dask cluster scheduler.

        The dask cluster scheduler will be launched as a HTCondor Job. Then, it
        will be enstablished a tunnel to communicate with it. Thus, the result
        scheduler job will be like a long running service.
        """
        if self.state == State.idle:
            self.start_sched_process.start()

            logger.debug("[_start][waiting for cluster id...]")
            while self.start_sched_process_q.empty():
                pass
            logger.debug("[_start][get cluster id]")
            self.cluster_id = self.start_sched_process_q.get()
            logger.debug(f"[_start][cluster_id: {self.cluster_id}")

            if self.asynchronous:
                self.scheduler_address = "Job submitted..."

            self.state = State.start

    @logger.catch
    async def _make_connections(self):
        # Prepare the ssh tunnel
        ssh_url = f"ssh-listener.{self.sshNamespace}.svc.cluster.local"

        logger.debug("[_start][Create ssh tunnel")
        logger.debug(f"[_start][url: {ssh_url}]")
        logger.debug(f"[_start][username: {self.name}]")
        logger.debug(f"[_start][password: {self.token}]")

        self.connection_process = ConnectionLoop(
            self.connection_process_q,
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
        while self.connection_process_q.empty():
            pass
        logger.debug("[_start][Check connection_q response]")
        started_tunnels = self.connection_process_q.get()
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
        logger.debug(f"[_start][tornado_address: http://localhost:{self.tornado_port}]")

        logger.debug("[_start][Test connections...]")
        connection_checks = False
        async with httpx.AsyncClient() as client:
            for attempt in range(6):
                await asyncio.sleep(2.0)

                logger.debug(f"[_start][Test connections: attempt {attempt}]")
                try:
                    target_url = f"http://localhost:{self.tornado_port}"
                    logger.debug(f"[_start][check controller][{target_url}]")
                    resp = await client.get(target_url)
                    logger.debug(
                        f"[_start][check controller][resp({resp.status_code})]"
                    )
                    if resp.status_code != 200:
                        logger.debug("[_start][Cannot connect to controller]")

                    target_url = self.dashboard_link
                    logger.debug(f"[_start][check dashboard][{target_url}]")
                    resp = await client.get(target_url)
                    logger.debug(f"[_start][check dashboard][resp({resp.status_code})]")
                    if resp.status_code != 200:
                        logger.debug("[_start][Cannot connect to dashboard]")
                except httpx.RemoteProtocolError:
                    pass
                else:
                    connection_checks = True
                    break

        if not connection_checks:
            raise Exception("Cannot check connections")

        await asyncio.sleep(2)
        self.state = State.running

    def close(self):
        if self.asynchronous:
            return self._close()
        else:
            cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
            cur_loop.run_until_complete(self._close())

    @logger.catch
    async def _close(self):
        if self.state == State.running:
            # Close the dask cluster
            target_url = f"http://127.0.0.1:{self.tornado_port}/close"
            logger.debug(f"[Scheduler][close][url: {target_url}]")

            async with httpx.AsyncClient() as client:
                resp = await client.get(target_url)
                logger.debug(
                    f"[Scheduler][close][resp({resp.status_code}): {resp.text}]"
                )

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

        if self.state == State.running:
            # Close scheduler connection
            self.connection_process_q.put("STOP")

        self.state = State.idle
        # To avoid wrong call on scheduler_info when make_cluster after deletion
        self.scheduler_address = ""

    @logger.catch
    def scale(self, n: int):
        if self.state == State.running:
            if self.asynchronous:
                return self._scale(n)
            else:
                cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
                cur_loop.run_until_complete(self._scale(n))
        else:
            raise Exception("Cluster is not yet running...")

    @logger.catch
    async def _scale(self, n: int):
        # Scale the cluster
        target_url = f"http://127.0.0.1:{self.tornado_port}/jobs?num={n}"
        logger.debug(f"[Scheduler][scale][num: {n}][url: {target_url}]")

        async with httpx.AsyncClient() as client:
            resp = await client.get(target_url)
            logger.debug(f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]")

    @logger.catch
    def adapt(self, minimum: int, maximum: int):
        if self.state == State.running:
            target_url = f"http://127.0.0.1:{self.tornado_port}/adapt?minimumJobs={minimum}&maximumJobs={maximum}"
            logger.debug(
                f"[Scheduler][adapt][minimum: {minimum}|maximum: {maximum}][url: {target_url}]"
            )
            resp = requests.get(target_url)
            logger.debug(f"[Scheduler][adapt][resp({resp.status_code}): {resp.text}]")
            if resp.status_code != 200:
                raise Exception("Cluster adapt failed...")

            return AdaptiveProp(minimum, maximum)
        else:
            raise Exception("Cluster is not yet running...")
