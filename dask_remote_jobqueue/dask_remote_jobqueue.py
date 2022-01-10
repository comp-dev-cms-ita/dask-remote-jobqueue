# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import asyncio
import json
import logging

# import math
import os
import weakref
from dataclasses import dataclass
from enum import Enum
from inspect import isawaitable
from multiprocessing import Queue
from queue import Empty
from random import randrange
from subprocess import STDOUT, check_output

import httpx
import requests
from loguru import logger

from .utils import ConnectionLoop, StartDaskScheduler


class State(Enum):
    idle = 1
    start = 2
    scheduler_up = 3
    waiting_connections = 4
    running = 5
    closing = 6
    error = 7


@dataclass
class AdaptiveProp:
    """Class for keeping track of minimum and maximum workers in adaptive mode."""

    minimum: int
    maximum: int


class RemoteHTCondor:

    """Class to manage a dask scheduler inside an HTCondor Cluster"""

    def __init__(
        self,
        ssh_namespace="default",
        user: str = "NONE",
        ssh_url_port: int = 8122,
        asynchronous: bool = True,  # Set by dask-labextension but not used in this class
        sitename: str = "",
        debug: bool = True,
    ):
        self.__debug = debug

        if self.__debug:
            logging.getLogger().setLevel(logging.DEBUG)
            try:
                logger.add(
                    "/var/log/RemoteHTCondor.log",
                    rotation="32 MB",
                    enqueue=True,
                    backtrace=True,
                    diagnose=True,
                )
            except PermissionError:
                logger.add(
                    "/tmp/log/RemoteHTCondor.log",
                    rotation="32 MB",
                    enqueue=True,
                    backtrace=True,
                    diagnose=True,
                )
        else:
            logging.getLogger().setLevel(logging.INFO)

        logger.info("[RemoteHTCondor][init]")

        # httpx client
        self.httpx_client = httpx.AsyncClient()

        # Inner class status
        self.state: State = State.idle
        self._job_status: str = ""
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
        self.sched_port: int = randrange(20000, 27000)
        self.dash_port: int = randrange(27000, 34000)
        self.controller_port: int = randrange(34000, 40000)

        logger.debug(f"generated -> sched_port: {self.sched_port}")
        logger.debug(f"generated -> dash_port: {self.dash_port}")
        logger.debug(f"generated -> controller_port: {self.controller_port}")

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
        self.htc_scitoken_file = "token"
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
        return self.controller_port

    @property
    def controller_address(self) -> str:
        return f"http://localhost:{self.controller_port}"

    @property
    def job_status(self) -> str:
        return self._job_status

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
                f = self.start()
                if isawaitable(f):
                    await f
            logger.debug(
                f"[RemoteHTCondor][__await__][closure EXIT][state: {self.state}]"
            )
            return self

        return closure().__await__()

    async def __aenter__(self):
        """Enable entering in the async context."""
        logger.debug(f"[RemoteHTCondor][__aenter__][state: {self.state}]")
        await self
        assert self.state == State.running
        return self

    async def __aexit__(self, *_):
        """Enable exiting from the async context."""
        logger.debug(f"[RemoteHTCondor][__aexit__][state: {self.state}]")
        if self.state == State.running:
            f = self.close()
            if isawaitable(f):
                await f
                await self.httpx_client.aclose()

    @property
    def scheduler_info(self) -> dict:
        logger.debug(f"[Scheduler][scheduler_info][state: {self.state}]")
        if self.state != State.running:
            if self.asynchronous:
                if self.state == State.start:
                    logger.debug(
                        "[Scheduler][scheduler_info][waiting for scheduler update...]"
                    )
                    try:
                        msg = self.start_sched_process_q.get_nowait()
                        if msg == "SCHEDULERJOB==IDLE":
                            self._job_status = "Job is idle"
                        elif msg == "SCHEDULERJOB==HOLD":
                            self._job_status = "Job is hold"
                        elif msg == "SCHEDULERJOB==RUNNING":
                            self.state = State.scheduler_up
                            self._job_status = "Waiting for connection"
                    except Empty:
                        logger.debug("[Scheduler][scheduler_info][empty queue...]")

                elif self.state == State.scheduler_up:
                    logger.debug("[Scheduler][scheduler_info][make connections...]")
                    cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
                    connection_done_event = asyncio.Event()
                    cur_loop.create_task(self._make_connections(connection_done_event))

                    async def callback_waiting_connection(connection_done_event):
                        await connection_done_event.wait()
                        is_ok = await self._connection_ok()
                        if is_ok:
                            self.state = State.running
                            self._job_status = "Running"
                        else:
                            self.state = State.error
                            self._job_status = "Error on connection..."
                            self.dashboard_link = ""

                    cur_loop.create_task(
                        callback_waiting_connection(connection_done_event)
                    )

                elif self.state == State.waiting_connections:
                    logger.debug(
                        "[Scheduler][scheduler_info][waiting for connection...]"
                    )
                    cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()

                    async def sleep_loop():
                        await asyncio.sleep(1.0)

                    cur_loop.create_task(sleep_loop())
                    num_points = (self._job_status.count(".") + 1) % 4
                    self._job_status = "Waiting for connection" + "." * num_points

            return self._scheduler_info

        # Check controller
        target_url = f"http://127.0.0.1:{self.controller_port}/"
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
        target_url = f"http://127.0.0.1:{self.controller_port}/schedulerID"
        logger.debug(f"[Scheduler][scheduler_info][url: {target_url}]")

        resp = requests.get(target_url)
        logger.debug(
            f"[Scheduler][scheduler_info][resp({resp.status_code}): {resp.text}]"
        )
        self._scheduler_info["id"] = resp.text

        # Update the worker specs
        target_url = f"http://127.0.0.1:{self.controller_port}/workerSpec"
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

        cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
        cur_loop.run_until_complete(self._start())

        msg = self.start_sched_process_q.get()
        while msg != "SCHEDULERJOB==RUNNING":
            msg = self.start_sched_process_q.get()

        self.state = State.scheduler_up
        return cur_loop.run_until_complete(self._make_connections())

    async def _start(self):
        """Start the dask cluster scheduler.

        The dask cluster scheduler will be launched as a HTCondor Job. Then, it
        will be enstablished a tunnel to communicate with it. Thus, the result
        scheduler job will be like a long running service.
        """
        if self.state == State.idle:
            self.state = State.start

            if not self.start_sched_process.is_alive():
                self.start_sched_process.start()

            await asyncio.sleep(1.0)
            logger.debug(
                f"[_start][start_sched_process: {self.start_sched_process.is_alive()}]"
            )

            logger.debug("[_start][waiting for cluster id...]")
            self.cluster_id = ""

            while not self.cluster_id:
                try:
                    msg = self.start_sched_process_q.get_nowait()
                    logger.debug(f"[_start][msg: {msg}]")
                    self.cluster_id = msg
                except Empty:
                    logger.debug("[_start][queue was empty...]")
                await asyncio.sleep(1.0)

            logger.debug(f"[_start][cluster_id: {self.cluster_id}")

            if self.asynchronous:
                self._job_status = "Job submitted"
                logger.info("[RemoteHTCondor][Job submitted]")

    async def _make_connections(self, connection_done_event: asyncio.Event = None):
        if self.state == State.scheduler_up:
            logger.info("[RemoteHTCondor][Make connections]")

            self.state = State.waiting_connections

            # Prepare the ssh tunnel
            ssh_url = f"ssh-listener.{self.sshNamespace}.svc.cluster.local"

            logger.debug("[_make_connections][Create ssh tunnel")
            logger.debug(f"[_make_connections][url: {ssh_url}]")
            logger.debug(f"[_make_connections][username: {self.name}]")
            logger.debug(f"[_make_connections][password: {self.token}]")

            self.connection_process = ConnectionLoop(
                self.connection_process_q,
                ssh_url=ssh_url,
                ssh_url_port=self.ssh_url_port,
                username=self.name,
                token=self.token,
                sched_port=self.sched_port,
                dash_port=self.dash_port,
                controller_port=self.controller_port,
            )
            logger.debug("[_make_connections][Start connection process]")

            try:
                self.connection_process.start()
            except AssertionError:
                logger.debug("[_make_connections][already started...]")

            logger.debug("[_make_connections][Wait for queue...]")
            started_tunnels = ""
            while not started_tunnels:
                try:
                    started_tunnels = self.connection_process_q.get_nowait()
                except Empty:
                    logger.debug("[_make_connections][queue was empty...]")
                await asyncio.sleep(1.0)

            logger.debug(f"[_make_connections][response: {started_tunnels}]")
            if started_tunnels != "OK":
                raise Exception("Cannot make any tunnel...")

            self.address = "localhost:{}".format(self.sched_port)
            self.dashboard_address = "http://localhost:{}".format(self.dash_port)

            logger.debug(f"[_make_connections][address: {self.address}]")
            logger.debug(
                f"[_make_connections][dashboard_address: {self.dashboard_address}]"
            )

            self.scheduler_address = self.address
            self.dashboard_link = f"{self.dashboard_address}/status"

            logger.debug(
                f"[_make_connections][scheduler_address: {self.scheduler_address}]"
            )
            logger.debug(f"[_make_connections][dashboard_link: {self.dashboard_link}]")
            logger.debug(
                f"[_make_connections][controller_address: http://localhost:{self.controller_port}]"
            )

            for attempt in range(10):
                logger.debug(f"[_make_connections][attempt: {attempt}]")
                if await self._connection_ok(1):
                    break
                await asyncio.sleep(6.0)
            else:
                raise Exception("Cannot check connections")

            self.state = State.running

            if connection_done_event:
                logger.debug("[_make_connections][connection_done_event: set]")
                connection_done_event.set()

    async def _connection_ok(self, attempts: int = 6) -> bool:
        logger.debug("[_connection_ok][run][Check job status]")
        cmd = "condor_q {}.0 -json".format(self.cluster_id)
        logger.debug(f"[_connection_ok][run][{cmd}]")

        cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
        logger.debug(f"[_connection_ok][run][{cmd_out.decode('ascii')}]")

        try:
            classAd = json.loads(cmd_out)
            logger.debug(f"[_connection_ok][run][classAd: {classAd}]")
        except Exception as cur_ex:
            logger.debug(f"[_connection_ok][run][{cur_ex}]")
            ex = Exception("Failed to decode claasAd for scheduler: %s" % cmd_out)
            raise ex

        job_status = classAd[0].get("JobStatus")
        logger.debug(f"[_connection_ok][job_status: {job_status}]")
        if job_status != 2:
            self.state = State.error
            self._job_status = "Scheduler Job exited with errors..."
            self.dashboard_link = ""

            return False

        logger.debug("[_connection_ok][Test connections...]")

        connection_checks = True

        for attempt in range(attempts):
            logger.debug(f"[_connection_ok][Test connections: attempt {attempt}]")
            try:
                target_url = f"http://localhost:{self.controller_port}"
                logger.debug(f"[_connection_ok][check controller][{target_url}]")
                resp = await self.httpx_client.get(target_url)
                logger.debug(
                    f"[_connection_ok][check controller][resp({resp.status_code})]"
                )
                if resp.status_code != 200:
                    logger.debug("[_connection_ok][Cannot connect to controller]")
            except Exception as ex:
                logger.debug(f"[_connection_ok][check controller][exception][{ex}]")
                connection_checks = connection_checks and False
            else:
                connection_checks = connection_checks and True

            try:
                logger.debug(
                    f"[_connection_ok][check dashboard][{self.dashboard_link}]"
                )
                resp = await self.httpx_client.get(self.dashboard_link)
                logger.debug(
                    f"[_connection_ok][check dashboard][resp({resp.status_code})]"
                )
                if resp.status_code != 200:
                    logger.debug("[_connection_ok][Cannot connect to dashboard]")
            except Exception as ex:
                logger.debug(f"[_connection_ok][check dashboard][exception][{ex}]")
                connection_checks = connection_checks and False
            else:
                connection_checks = connection_checks and True

            if connection_checks:
                break

            await asyncio.sleep(2.4)

        return connection_checks

    def close(self):
        if self.asynchronous:
            return self._close()

        cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
        return cur_loop.run_until_complete(self._close())

    async def _close(self):
        if self.state != State.closing:
            logger.info("[RemoteHTCondor][Closing...]")

            was_running = self.state == State.running
            self.state = State.closing

            self.scheduler_address: str = ""
            self.dashboard_link: str = ""
            self._job_status = "Closing"

            if was_running:
                # Close the dask cluster
                target_url = f"http://127.0.0.1:{self.controller_port}/scaleZeroAndClose?clusterID={self.cluster_id}"
                logger.debug(f"[Scheduler][close][url: {target_url}]")

                resp = await self.httpx_client.get(target_url)
                logger.debug(
                    f"[Scheduler][close][resp({resp.status_code}): {resp.text}]"
                )

                self.connection_process_q.put("STOP")

            self.state = State.idle

            logger.info("[RemoteHTCondor][Closed!]")

    def scale(self, n: int):
        # Scale the cluster
        if self.state != State.running:
            raise Exception(
                "Cluster is not completely up and running. Try again later..."
            )

        target_url = f"http://127.0.0.1:{self.controller_port}/jobs?num={n}"
        logger.debug(f"[Scheduler][scale][num: {n}][url: {target_url}]")

        if self.asynchronous:

            async def callScale():
                connected: bool = await self._connection_ok()
                if not connected:
                    raise Exception("Cluster is not reachable...")

                logger.debug("[Scheduler][scale][connection OK!]")

                resp = await self.httpx_client.get(target_url)

                if resp.status_code != 200:
                    raise Exception("Cluster scale failed...")

                logger.debug(
                    f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]"
                )

            return callScale()

        logger.debug("[Scheduler][scale][check connection...]")
        cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
        connected = cur_loop.run_until_complete(self._connection_ok())

        if not connected:
            raise Exception("Cluster is not reachable...")

        logger.debug("[Scheduler][scale][connection OK!]")

        resp = requests.get(target_url)
        if resp.status_code != 200:
            raise Exception("Cluster scale failed...")

        logger.debug(f"[Scheduler][scale][resp({resp.status_code}): {resp.text}]")

    def adapt(self, minimum: int, maximum: int):
        if self.state != State.running:
            raise Exception(
                "Cluster is not completely up and running. Try again later..."
            )

        target_url = f"http://127.0.0.1:{self.controller_port}/adapt?minimumJobs={minimum}&maximumJobs={maximum}"
        logger.debug(
            f"[Scheduler][adapt][minimum: {minimum}|maximum: {maximum}][url: {target_url}]"
        )

        cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()

        if self.asynchronous:

            async def callAdapt():
                connected: bool = await self._connection_ok()
                if not connected:
                    raise Exception("Cluster is not reachable...")

                logger.debug("[Scheduler][adapt][connection OK!]")

                resp = await self.httpx_client.get(target_url)
                if resp.status_code != 200:
                    raise Exception("Cluster adapt failed...")

                logger.debug(
                    f"[Scheduler][adapt][resp({resp.status_code}): {resp.text}]"
                )

            cur_loop.create_task(callAdapt())

            return AdaptiveProp(minimum, maximum)

        logger.debug("[Scheduler][adapt][check connection...]")
        connected = cur_loop.run_until_complete(self._connection_ok())
        if not connected:
            raise Exception("Cluster is not reachable...")

        logger.debug("[Scheduler][adapt][connection OK!]")

        resp = requests.get(target_url)
        logger.debug(f"[Scheduler][adapt][resp({resp.status_code}): {resp.text}]")
        if resp.status_code != 200:
            raise Exception("Cluster adapt failed...")

        return AdaptiveProp(minimum, maximum)
