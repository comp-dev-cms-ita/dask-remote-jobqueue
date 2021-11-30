import asyncio
import json
import os
import tempfile
from multiprocessing import Process, Queue
from subprocess import STDOUT, check_output
from time import sleep

import asyncssh
from jinja2 import Environment, PackageLoader, select_autoescape
from loguru import logger
import weakref


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


class StartDaskScheduler(Process):

    """Class to control the tunneling processes."""

    def __init__(
        self,
        remoteHTCondor: "weakref.ProxyType",
        queue: "Queue",
        environ: "os._Environ",
    ):
        logger.debug("[StartDaskScheduler][init]")
        super().__init__()
        self._remoteHTCondor: "weakref.ProxyType" = remoteHTCondor
        self._queue: "Queue" = queue
        self._environ: "os._Environ" = environ

        self._cluster_id: str = ""
        self._sitename: str = ""
        self._token: str = ""
        self._sched_port: int = -1
        self._dash_port: int = -1
        self._tornado_port: int = -1
        self._refresh_token: str = ""
        self._iam_server: str = ""
        self._client_id: str = ""
        self._client_secret: str = ""
        self._htc_ca: str = ""
        self._htc_debug: str = ""
        self._htc_collector: str = ""
        self._htc_schedd_host: str = ""
        self._htc_schedd_name: str = ""
        self._htc_scitoken_file: str = ""
        self._htc_sec_method: str = ""

    def _copy_attributes(self):
        self._sitename = getattr(self._remoteHTCondor, "sitename")
        logger.debug(f"[StartDaskScheduler][copy of sitename: {self._sitename}]")
        self._name = getattr(self._remoteHTCondor, "name")
        logger.debug(f"[StartDaskScheduler][copy of name: {self._name}]")
        self._token = getattr(self._remoteHTCondor, "token")
        logger.debug(f"[StartDaskScheduler][copy of token: {self._token}]")
        self._sched_port = getattr(self._remoteHTCondor, "sched_port")
        logger.debug(f"[StartDaskScheduler][copy of sched_port: {self._sched_port}]")
        self._dash_port = getattr(self._remoteHTCondor, "dash_port")
        logger.debug(f"[StartDaskScheduler][copy of dash_port: {self._dash_port}]")
        self._tornado_port = getattr(self._remoteHTCondor, "tornado_port")
        logger.debug(
            f"[StartDaskScheduler][copy of tornado_port: {self._tornado_port}]"
        )
        self._refresh_token = getattr(self._remoteHTCondor, "refresh_token")
        logger.debug(
            f"[StartDaskScheduler][copy of refresh_token: {self._refresh_token}]"
        )
        self._iam_server = getattr(self._remoteHTCondor, "iam_server")
        logger.debug(f"[StartDaskScheduler][copy of iam_server: {self._iam_server}]")
        self._client_id = getattr(self._remoteHTCondor, "client_id")
        logger.debug(f"[StartDaskScheduler][copy of client_id: {self._client_id}]")
        self._client_secret = getattr(self._remoteHTCondor, "client_secret")
        logger.debug(
            f"[StartDaskScheduler][copy of client_secret: {self._client_secret}]"
        )
        self._htc_ca = getattr(self._remoteHTCondor, "htc_ca")
        logger.debug(f"[StartDaskScheduler][copy of htc_ca: {self._htc_ca}]")
        self._htc_debug = getattr(self._remoteHTCondor, "htc_debug")
        logger.debug(f"[StartDaskScheduler][copy of htc_debug: {self._htc_debug}]")
        self._htc_collector = getattr(self._remoteHTCondor, "htc_collector")
        logger.debug(
            f"[StartDaskScheduler][copy of htc_collector: {self._htc_collector}]"
        )
        self._htc_schedd_host = getattr(self._remoteHTCondor, "htc_schedd_host")
        logger.debug(
            f"[StartDaskScheduler][copy of htc_schedd_host: {self._htc_schedd_host}]"
        )
        self._htc_schedd_name = getattr(self._remoteHTCondor, "htc_schedd_name")
        logger.debug(
            f"[StartDaskScheduler][copy of htc_schedd_name: {self._htc_schedd_name}]"
        )
        self._htc_scitoken_file = getattr(self._remoteHTCondor, "htc_scitoken_file")
        logger.debug(
            f"[StartDaskScheduler][copy of htc_scitoken_file: {self._htc_scitoken_file}]"
        )
        self._htc_sec_method = getattr(self._remoteHTCondor, "htc_sec_method")
        logger.debug(
            f"[StartDaskScheduler][copy of htc_sec_method: {self._htc_sec_method}]"
        )

    def run(self):
        self._copy_attributes()
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
            if self._sitename:
                selected_sitename = f'requirements = ( SiteName == "{self._sitename}" )'

            for f in files:
                tmpl = env.get_template(f)
                with open(tmpdirname + "/" + f, "w") as dest:
                    render = tmpl.render(
                        name=self._name,
                        token=self._token,
                        sched_port=self._sched_port,
                        dash_port=self._dash_port,
                        tornado_port=self._tornado_port,
                        refresh_token=self._refresh_token,
                        iam_server=self._iam_server,
                        client_id=self._client_id,
                        client_secret=self._client_secret,
                        htc_ca=self._htc_ca,
                        htc_debug=self._htc_debug,
                        htc_collector=self._htc_collector,
                        htc_schedd_host=self._htc_schedd_host,
                        htc_schedd_name=self._htc_schedd_name,
                        htc_scitoken_file=self._htc_scitoken_file,
                        htc_sec_method=self._htc_sec_method,
                        selected_sitename=selected_sitename,
                    )

                    logger.debug(f"[StartDaskScheduler][run][{dest.name}]")
                    logger.debug(f"[StartDaskScheduler][run][\n{render}\n]")

                    dest.write(render)

            cmd = "cd {}; condor_submit -spool scheduler.sub".format(tmpdirname)

            # Submit HTCondor Job to start the scheduler
            try:
                logger.debug(
                    f"[StartDaskScheduler][run][{cmd}][environ: {self._environ}]"
                )
                cmd_out = check_output(
                    cmd, stderr=STDOUT, shell=True, env=self._environ
                )
                logger.debug(f"[StartDaskScheduler][run][{cmd_out.decode('ascii')}]")
            except Exception as ex:
                raise ex

            try:
                self._cluster_id = str(cmd_out).split("cluster ")[1].strip(".\\n'")
                logger.debug(f"[StartDaskScheduler][run][jobid: {self._cluster_id}.0]")
            except Exception:
                ex = Exception("Failed to submit job for scheduler: %s" % cmd_out)
                raise ex

            if not self._cluster_id:
                ex = Exception("Failed to submit job for scheduler: %s" % cmd_out)
                raise ex

            self._queue.put(self._cluster_id)

        # Wait for the job to be running
        job_status = 1

        # While job is idle or hold
        while job_status in [1, 5]:
            sleep(6.0)

            logger.debug("[StartDaskScheduler][run][Check job status]")
            cmd = "condor_q {}.0 -json".format(self._cluster_id)
            logger.debug(f"[StartDaskScheduler][run][{cmd}]")

            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
            logger.debug(f"[StartDaskScheduler][run][{cmd_out.decode('ascii')}]")

            try:
                classAd = json.loads(cmd_out)
                logger.debug(f"[StartDaskScheduler][run][classAd: {classAd}]")
            except Exception as cur_ex:
                logger.debug(f"[StartDaskScheduler][run][{cur_ex}]")
                ex = Exception("Failed to decode claasAd for scheduler: %s" % cmd_out)
                raise ex

            job_status = classAd[0].get("JobStatus")
            logger.debug(f"[StartDaskScheduler][run][job_status: {job_status}]")
            if job_status == 1:
                logger.debug(
                    f"[StartDaskScheduler][run][jobid: {self._cluster_id}.0 -> still idle]"
                )
                continue
            elif job_status == 5:
                logger.debug(
                    f"[StartDaskScheduler][run][jobid: {self._cluster_id}.0 -> still hold]"
                )
                continue
            elif job_status != 2:
                ex = Exception("Scheduler job in error {}".format(job_status))
                raise ex

        sleep(2.0)
        logger.debug(
            f"[StartDaskScheduler][run][jobid: {self._cluster_id}.0 -> {job_status}]"
        )
