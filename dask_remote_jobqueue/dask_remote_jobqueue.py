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
from typing import Union

import httpx
import requests
from loguru import logger

from .utils import ConnectionManager, StartDaskScheduler


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
        ssh_url: str = "",
        ssh_url_port: int = 8122,
        asynchronous: bool = True,  # Set by dask-labextension but not used in this class
        sitename: str = "",
        singularity_wn_image = "/cvmfs/images.dodas.infn.it/registry.hub.docker.com/dodasts/root-in-docker:ubuntu22-kernel-v1",
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
        # timeout = httpx.Timeout(5.0)
        # self.httpx_client = httpx.AsyncClient(timeout=timeout)
        self.httpx_client = httpx.AsyncClient()

        # Inner class status
        self.state: State = State.idle
        self._job_status: str = ""
        self.asynchronous: bool = asynchronous

        self.connection_process_q: "Queue" = Queue()
        self.connection_manager_q: "Queue" = Queue()
        self.connection_manager: Union["ConnectionManager", None] = None
        self.start_sched_process_q: "Queue" = Queue()
        self.start_sched_process: Union["StartDaskScheduler", None] = None

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
        self.ssh_url: str = ssh_url
        self.ssh_url_port: int = ssh_url_port

        self.cluster_id: str = ""
        self.sitename: str = sitename
        self.singularity_wn_image: str = singularity_wn_image

        # Jupyter env vars
        self.username = (
            os.environ.get("JUPYTERHUB_USER", user) + f"-{self.sched_port}.dask-ssh"
        )
        self.dash_hostname = (
            os.environ.get("JUPYTERHUB_USER", user) + f"-{self.dash_port}.dash.dask-ssh"
        )

        self.ssh_namespace = os.environ.get("SSH_NAMESPACE", ssh_namespace)

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
        self._scheduler_info: dict = {
            "type": "Scheduler",
            "id": None,
            "address": "",
            "services": {},
            "workers": {},
        }
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
                        num_points = (self._job_status.count(".") + 1) % 4
                        if msg == "SCHEDULERJOB==IDLE":
                            self._job_status = "Job is idle" + "." * num_points
                        elif msg == "SCHEDULERJOB==HOLD":
                            self._job_status = "Job is hold" + "." * num_points
                        elif msg == "SCHEDULERJOB==RUNNING":
                            self.state = State.scheduler_up
                            self._job_status = "Start connection"
                    except Empty:
                        logger.debug("[Scheduler][scheduler_info][empty queue...]")

                elif self.state == State.scheduler_up:
                    logger.debug("[Scheduler][scheduler_info][make connections...]")
                    cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
                    cur_loop.create_task(self._make_connections())

                elif self.state == State.waiting_connections:
                    logger.debug(
                        "[Scheduler][scheduler_info][waiting for connection...]"
                    )
                    try:
                        msg = self.connection_manager_q.get_nowait()
                        if msg == "OK":
                            cur_loop: "asyncio.AbstractEventLoop" = (
                                asyncio.get_event_loop()
                            )
                            self._post_connection()
                            self.state = State.running
                            self._job_status = "Running"
                        else:
                            self.state = State.error
                            self._job_status = msg
                    except Empty:
                        logger.debug("[Scheduler][scheduler_info][empty queue...]")
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

        if self._scheduler_info["address"] == "":
            self._scheduler_info["address"] = f"tcp://127.0.0.1:{self.sched_port}"
        if self._scheduler_info["services"] == {}:
            self._scheduler_info["services"] = {"dashboard": self.dash_port}

        try:
            if self._scheduler_info["id"] is None:
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
            if resp.status_code != 304:
                self._scheduler_info["workers"] = json.loads(resp.text)
        except requests.RequestException as exc:
            logger.debug(f"[Scheduler][scheduler_info][error: {exc}]")
            self._job_status = "Connection error..."

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
        cur_loop.run_until_complete(self._make_connections())

        msg = self.connection_manager_q.get()
        if msg != "OK":
            self.state = State.error
            self._job_status = msg
        else:
            self._post_connection()
            self.state = State.running
            self._job_status = "Running"

    async def _start(self):
        """Start the dask cluster scheduler.

        The dask cluster scheduler will be launched as a HTCondor Job. Then, it
        will be enstablished a tunnel to communicate with it. Thus, the result
        scheduler job will be like a long running service.
        """
        if self.state == State.idle:
            self.state = State.start

            if self.start_sched_process is None:
                self.start_sched_process = StartDaskScheduler(
                    weakref.proxy(self), self.start_sched_process_q, os.environ, self.singularity_wn_image
                )
                logger.debug("[_start][start sched process...]")
                self.start_sched_process.start()

            logger.debug("[_start][waiting for cluster id...]")
            self.cluster_id = ""

            while not self.cluster_id:
                proc_alive = self.start_sched_process.is_alive()
                logger.debug(f"[_start][sched_process alive: {proc_alive}]")
                try:
                    msg = self.start_sched_process_q.get_nowait()
                    logger.debug(f"[_start][msg: {msg}]")
                    self.cluster_id = msg
                except Empty:
                    logger.debug("[_start][queue was empty...]")
                await asyncio.sleep(2.4)

            logger.debug(f"[_start][cluster_id: {self.cluster_id}")

            if self.asynchronous:
                self._job_status = "Job submitted"
                logger.info("[RemoteHTCondor][Job submitted]")

    def _post_connection(self):
        self.address = "localhost:{}".format(self.sched_port)
        self.dashboard_address = "http://localhost:{}".format(self.dash_port)

        logger.debug(f"[_post_connection][address: {self.address}]")
        logger.debug(f"[_post_connection][dashboard_address: {self.dashboard_address}]")

        self.scheduler_address = self.address
        self.dashboard_link = f"{self.dashboard_address}/status"

        logger.debug(f"[_post_connection][scheduler_address: {self.scheduler_address}]")
        logger.debug(f"[_post_connection][dashboard_link: {self.dashboard_link}]")
        logger.debug(
            f"[_post_connection][controller_address: http://localhost:{self.controller_port}]"
        )

        self.connection_manager = None

    async def _make_connections(self):
        if self.state == State.scheduler_up:
            logger.info("[RemoteHTCondor][Make connections]")

            self.state = State.waiting_connections

            if not self.connection_manager:
                self._job_status = "Start connection manager"

                self.connection_manager = ConnectionManager(
                    self.connection_manager_q,
                    self.connection_process_q,
                    cluster_id=self.cluster_id,
                    ssh_namespace=self.ssh_namespace,
                    ssh_url=self.ssh_url,
                    ssh_url_port=self.ssh_url_port,
                    username=self.username,
                    token=self.token,
                    sched_port=self.sched_port,
                    dash_port=self.dash_port,
                    controller_port=self.controller_port,
                )
                logger.debug("[_make_connections][Start connection manager]")
                self.connection_manager.start()

    async def _connection_ok(
        self, attempts: int = 6, only_controller: bool = False
    ) -> bool:

        logger.debug("[_connection_ok][run][Check job status]")
        cmd = "condor_q {}.0 -json".format(self.cluster_id)
        logger.debug(f"[_connection_ok][run][{cmd}]")

        if not only_controller:
            cmd_out = ""
            try:
                cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
                logger.debug(f"[_connection_ok][run][{cmd_out.decode('ascii')}]")
            except Exception as cur_ex:
                logger.debug(f"[_connection_ok][run][{cur_ex}][{cmd_out}]")
                self._job_status = "Failed to condor_q..."
                self.state = State.error
                return False

            try:
                classAd = json.loads(cmd_out)
                logger.debug(f"[_connection_ok][run][classAd: {classAd}]")
            except Exception as cur_ex:
                logger.debug(f"[_connection_ok][run][{cur_ex}][{cmd_out}]")
                self._job_status = "Failed to decode claasAd..."
                self.state = State.error
                return False

            job_status = classAd[0].get("JobStatus")
            logger.debug(f"[_connection_ok][job_status: {job_status}]")
            if job_status != 2:
                self.state = State.error
                self._job_status = "Scheduler Job exited with errors..."
                self.dashboard_link = ""

                return False

        logger.debug("[_connection_ok][Test connections...]")
        connection_checks: bool = True

        for attempt in range(attempts):
            await asyncio.sleep(2.4)

            connection_checks = True
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
            except (OSError, httpx.HTTPError) as ex:
                logger.debug(f"[_connection_ok][check controller][exception][{ex}]")
                connection_checks &= False
            else:
                connection_checks &= True

            if not only_controller:
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
                except (OSError, httpx.HTTPError) as ex:
                    logger.debug(f"[_connection_ok][check dashboard][exception][{ex}]")
                    connection_checks &= False
                else:
                    connection_checks &= True

            logger.debug(
                f"[_connection_ok][Test connections: attempt {attempt}][connection: {connection_checks}]"
            )

            if connection_checks:
                break

        return connection_checks

    def close(self):
        if self.state != State.closing and self.state != State.idle:
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

                try:
                    resp = requests.get(target_url)
                    logger.debug(
                        f"[Scheduler][close][resp({resp.status_code}): {resp.text}]"
                    )
                except requests.RequestException as exc:
                    logger.debug(f"[Scheduler][close][error: {exc}]")
                    raise

                self.connection_process_q.put("STOP")

            self._scheduler_info: dict = {
                "type": "Scheduler",
                "id": None,
                "address": "",
                "services": {},
                "workers": {},
            }

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

        try:
            resp = requests.get(target_url)
            logger.debug(f"[Scheduler][scale][resp: {resp.status_code}]")
            if resp.status_code != 200:
                raise Exception("Cluster scale failed...")
        except requests.RequestException as exc:
            logger.debug(f"[Scheduler][scale][error: {exc}]")
            raise

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

        try:
            resp = requests.get(target_url)
            logger.debug(f"[Scheduler][adapt][resp({resp.status_code}): {resp.text}]")
            if resp.status_code != 200:
                raise Exception("Cluster adapt failed...")
        except requests.RequestException as exc:
            logger.debug(f"[Scheduler][adapt][error: {exc}]")
            raise

        return AdaptiveProp(minimum, maximum)
