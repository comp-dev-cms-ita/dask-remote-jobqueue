# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import asyncio
import json
import logging
import math
import os
from contextlib import suppress
from inspect import isawaitable
from multiprocessing import Process, Queue
from subprocess import STDOUT, check_output
from time import sleep


import asyncssh
import dask.config
import tornado.ioloop
import tornado.web
import yaml
from dask.distributed import Status
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

with open("config.yaml") as file_:
    defaults = yaml.safe_load(file_)

dask.config.update(dask.config.config, defaults)

logger.debug(f"[dask][config][default][{dask.config.config}]")


class MyHTCondorJob(HTCondorJob):
    submit_command = "./job_submit.sh"
    cancel_command = "./job_rm.sh"
    executable = "/bin/bash"

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            death_timeout=60 * 5,  # 5min
            python="python3",
        )


##
# List of variables set in scheduler.sub
#
# JHUB_TOKEN={{ token }}
# JHUB_USER={{ name }}
# SCHED_PORT={{ sched_port }}
# DASH_PORT={{ dash_port }}
# RT={{ refresh_token }}
# IAM_SERVER={{ iam_server }}
# IAM_CLIENT_ID={{ client_id }}
# IAM_CLIENT_SECRET={{ client_secret }}
# _condor_AUTH_SSL_CLIENT_CAFILE={{ htc_ca }};
# _condor_TOOL_DEBUG={{ htc_debug }};
# _condor_COLLECTOR_HOST={{ htc_collector }};
#  _condor_SCHEDD_HOST={{ htc_schedd_host }};
# _condor_SCHEDD_NAME={{ htc_schedd_name }};
# _condor_SCITOKENS_FILE={{ htc_scitoken_file }};
# _condor_SEC_DEFAULT_AUTHENTICATION_METHODS={{ htc_sec_method}}

htc_ca = "./ca.crt"
# os.environ.get("_condor_AUTH_SSL_CLIENT_CAFILE")
htc_debug = os.environ.get("_condor_TOOL_DEBUG")
htc_collector = os.environ.get("_condor_COLLECTOR_HOST")
htc_schedd_host = os.environ.get("_condor_SCHEDD_HOST")
htc_schedd_name = os.environ.get("_condor_SCHEDD_NAME")
htc_scitoken_file = "./token"
# os.environ.get("_condor_SCITOKENS_FILE")
htc_sec_method = os.environ.get("_condor_SEC_DEFAULT_AUTHENTICATION_METHODS")
token = os.environ.get("JHUB_TOKEN")
name = os.environ.get("JHUB_USER", "")
sched_port = int(os.environ.get("SCHED_PORT", "42000"))
dash_port = int(os.environ.get("DASH_PORT", "42001"))
controller_port = int(os.environ.get("CONTROLLER_PORT", "42002"))

logger.debug(f"name: {name}")
logger.debug(f"token: {token}")

##
# Local testing
# controller_port = 8181

site = None
machine_ad_file = os.environ.get("_CONDOR_MACHINE_AD", "")

if machine_ad_file:
    with open(machine_ad_file) as f:
        lines = f.readlines()
        for line in lines:
            if line.startswith("SiteName"):
                site = line.split("= ")[1].strip("\n").strip()
                break

logger.debug(f"SiteName is: {site}")

scheduler_options_vars = {
    "host": ":{}".format(sched_port),
    "dashboard_address": "127.0.0.1:{}".format(dash_port),
    "idle_timeout": "4h",
}
job_extra_vars = {
    "+OWNER": '"' + name.split("-")[0] + '"',
    "+SingularityImage": "/cvmfs/images.dodas.infn.it/registry.hub.docker.com/dodasts/root-in-docker:ubuntu22-v1",
    "log": "wn.log",
    "output": "wn.out",
    "error": "wn.error",
    "should_transfer_files": "YES",
    "when_to_transfer_output": "ON_EXIT"
}

if site:
    job_extra_vars["requirements"] = f"( SiteName == {site} )"

##
# Local testing
#
# from dask.distributed import LocalCluster
# cluster = LocalCluster(n_workers=1, processes=False)

logger.debug(f"[dask][config][{dask.config.config}]")


class SchedulerProc(Process):
    def __init__(self, sched_q: Queue, controller_q: Queue):
        super().__init__()

        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q
        self.__running: bool = False
        self.cluster = None
        self.adaptive = None
        self._worker_spec_json = ""
        logger.debug(f"[SchedulerProc][dask][config][{dask.config.config}]")

    def _worker_spec(self) -> dict:
        workers = {}
        for num, (worker_name, spec) in enumerate(self.cluster.worker_spec.items()):
            logger.debug(f"[workerSpec][{num}][{worker_name}][{spec}]")
            memory_limit = spec["options"].get("memory", "").lower()
            if not memory_limit:
                memory_limit = spec["options"].get("memory_limit", "")
            if isinstance(memory_limit, str):
                if memory_limit.find("gb") != -1:
                    memory_limit = int(memory_limit.replace("gb", "").strip()) * 10 ** 9
                elif memory_limit.find("gib") != -1:
                    memory_limit = (
                        int(memory_limit.replace("gib", "").strip()) * 2 ** 30
                    )
                else:
                    memory_limit = -1
            elif not isinstance(memory_limit, int):
                memory_limit = -1
            # See dask-labextension make_cluster_model
            n_cores = spec["options"].get("cores", -1)
            if n_cores == -1:
                n_cores = spec["options"].get("nthreads", -1)
            workers[worker_name] = {
                "nthreads": n_cores,
                "memory_limit": memory_limit,
            }
        return workers

    def run(self):
        self.cluster = HTCondorCluster(
            job_cls=MyHTCondorJob,
            cores=1,
            memory="2 GiB",  # ref: https://github.com/dask/dask/blob/e4799c0498b5e5877705bb5542d8d01116ee1320/dask/utils.py#L1404
            disk="1 GB",
            scheduler_options=scheduler_options_vars,
            job_extra=job_extra_vars,
            # silence_logs="debug",
            local_directory="./scratch",
        )

        while self.cluster.status != Status.running:
            logger.debug(f"[SchedulerProc][status: {self.cluster.status}]")
            sleep(1)

        self.__running = True
        self.controller_q.put("READY")
        clusterID: str = ""

        while self.__running:
            msg = self.sched_q.get()
            logger.debug(f"[SchedulerProc][msg: {msg}]")
            if msg["op"] == "job_script":
                job_script = self.cluster.job_script()
                logger.debug(f"[SchedulerProc][job_script][resp: {job_script}]")
                self.controller_q.put(job_script)
            elif msg["op"] == "sched_id":
                logger.debug(
                    f"[SchedulerProc][sched_id][resp: {self.cluster.scheduler.id}]"
                )
                self.controller_q.put(self.cluster.scheduler.id)
            elif msg["op"] == "close":
                self.__running = False
                logger.debug("[SchedulerProc][close]")
                with suppress(AssertionError):  # If some jobs are not yet killed
                    self.cluster.close()
            elif msg["op"] == "scaleZeroAndClose":
                logger.debug("[SchedulerProc][scaling to 0]")
                clusterID = msg["clusterID"]
                self.cluster.scale(jobs=0)
                while len(self.cluster.worker_spec) > 0:
                    logger.debug(
                        f"[SchedulerProc][workers to kill: {len(self.cluster.worker_spec)}]"
                    )
                sleep(6)
                self.__running = False
                logger.debug("[SchedulerProc][close]")
                with suppress(AssertionError):  # If some jobs are not yet killed
                    self.cluster.close()
            elif msg["op"] == "adapt":
                logger.debug(
                    f"[SchedulerProc][adapt {msg['minimum_jobs']}-{msg['maximum_jobs']}]"
                )
                self.adaptive = self.cluster.adapt(
                    minimum_jobs=msg["minimum_jobs"],
                    maximum_jobs=msg["maximum_jobs"],
                )
            elif msg["op"] == "scale_jobs":
                logger.debug(f"[SchedulerProc][scale to {msg['num']} jobs]")
                if self.adaptive:
                    self.adaptive.stop()
                    self.adaptive = None
                self.cluster.scale(jobs=msg["num"])
            elif msg["op"] == "scale_workers":
                logger.debug(f"[SchedulerProc][scale to {msg['num']} workers]")
                self.cluster.scale(n=msg["num"])
            elif msg["op"] == "worker_spec":
                specs = self._worker_spec()
                cur_specs = json.dumps(specs)
                if cur_specs != self._worker_spec_json:
                    self._worker_spec_json = cur_specs
                    logger.debug(
                        f"[SchedulerProc][worker_spec][resp: {self._worker_spec_json}]"
                    )
                    self.controller_q.put(self._worker_spec_json)
                else:
                    logger.debug(
                        f"[SchedulerProc][worker_spec][NOTMODIFIED][resp: {self._worker_spec_json}]"
                    )
                    self.controller_q.put("NOTMODIFIED")
            elif msg["op"] == "logs":
                # https://github.com/dask/distributed/blob/55ff8337e95a55abf9268c19342572a09e1066ac/distributed/deploy/cluster.py#L295
                cluster_logs: str = self.cluster.get_logs(
                    cluster=True, scheduler=False, workers=False
                ).get("Cluster", "")
                scheduler_logs: list[tuple] = self.cluster.scheduler.get_logs()
                worker_logs: dict = self.cluster.scheduler.get_worker_logs()
                nanny_logs: dict = self.cluster.scheduler.get_worker_logs(nanny=True)

                if isawaitable(worker_logs):
                    cur_loop: "asyncio.AbstractEventLoop" = asyncio.get_event_loop()
                    worker_logs = cur_loop.run_until_complete(worker_logs)
                    nanny_logs = cur_loop.run_until_complete(nanny_logs)

                self.controller_q.put(
                    {
                        "cluster_logs": cluster_logs,
                        "scheduler_logs": scheduler_logs,
                        "worker_logs": worker_logs,
                        "nanny_logs": nanny_logs,
                    }
                )

        logger.debug("[SchedulerProc][exit]")

        sleep(14)

        logger.debug(f"[SchedulerProc][rm cluster][{clusterID}]")
        # Remove the HTCondor dask scheduler job
        cmd = f"./job_rm.sh {clusterID}.0"
        logger.debug(f"[SchedulerProc][cmd: {cmd}]")

        try:
            cmd_out = check_output(cmd, stderr=STDOUT, shell=True)
            logger.debug(str(cmd_out))
        except Exception as ex:
            raise ex

        if str(cmd_out) != "b'Job {}.0 marked for removal\\n'".format(clusterID):
            raise Exception("Failed to remove scheduler job: %s" % cmd_out)


async def tunnel_scheduler():
    logger.debug("start tunnel scheduler")
    connection = await asyncssh.connect(
        "jhub.131.154.96.124.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", sched_port, "0.0.0.0", sched_port
    )
    await forwarder.wait_closed()


async def tunnel_dashboard():
    logger.debug("start tunnel dashboard")
    connection = await asyncssh.connect(
        "jhub.131.154.96.124.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", dash_port, "0.0.0.0", dash_port
    )
    await forwarder.wait_closed()


async def tunnel_controller():
    logger.debug("start tunnel controller")
    connection = await asyncssh.connect(
        "jhub.131.154.96.124.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", controller_port, "0.0.0.0", controller_port
    )
    await forwarder.wait_closed()


async def start_controller(sched_q: Queue, controller_q: Queue):
    logger.debug(f"start controller web: 127.0.0.1:{controller_port}")
    app = make_app(sched_q, controller_q)
    app.listen(controller_port, address="0.0.0.0")


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(
            "Hello, may I help you? I guess not, you're in the wrong place... 6u"
        )


class JobScriptHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        logger.debug("[JobScriptHandler][send][op: job_script]")
        self.sched_q.put({"op": "job_script"})
        res = self.controller_q.get()
        logger.debug(f"[JobScriptHandler][res: {res}]")
        self.write(res)


class CloseHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        logger.debug("[CloseHandler][send][op: close]")
        self.sched_q.put({"op": "close"})
        self.write("cluster is exiting")


class ScaleZeroAndCloseHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        clusterID = self.get_argument("clusterID")
        logger.debug("[ScaleZeroAndCloseHandler][send][op: scaleZeroAndClose]")
        self.sched_q.put({"op": "scaleZeroAndClose", "clusterID": clusterID})
        self.write("cluster is scaling down and exiting")

    def prepare(self):
        logger.debug(self.request.arguments)


class LogsHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    async def get(self):
        logger.debug("[LogsHandler][send][op: logs]")
        self.sched_q.put({"op": "logs"})
        res = self.controller_q.get()
        cluster_logs: str = res["cluster_logs"]
        scheduler_logs: list[tuple] = res["scheduler_logs"]
        worker_logs: dict = res["worker_logs"]
        nanny_logs: dict = res["nanny_logs"]
        self.write(
            """<!DOCTYPE html>
            <html>
            <head>
            <title>Logs</title>
            <!--<meta http-equiv="refresh" content="60">-->
            </head>
            <style>
            .collapsible {
                background-color: #ecb172;
                color: black;
                font-weight: bold;
                cursor: pointer;
                padding: 18px;
                width: 100%;
                border: none;
                text-align: left;
                outline: none;
                font-size: 15px;
                border-bottom: 
            }

            .active, .collapsible:hover {
                background-color: #ec8f72;
            }

            .content {
                padding: 0 18px;
                display: none;
                overflow: hidden;
                background-color: #fafafa;
            }

            table, th, td {
                border: 1px solid black;
            }

            table {
                width: 100%;
            }
            
            tr:nth-child(even) {
                background-color: #ffd9b1;
            }
            
            .sticky {
                position: fixed;
                top: 0;
                width: 100%;
            }
            
            .header {
                background: #ecb172;
                padding: 6px;
                border-radius: 6px;
                margin-bottom: 1em;
            }
            
            #reload {
                font-weight: bold; 
                font-size: larger;
                position: absolute;
                right: 0;
                padding-right: 1em;
            }
            </style>
            <body>
            """
        )
        self.write('<div class="header" id="myHeader"><p>')
        self.write('<span style="padding-left: 1em;">Go to &rarr; </span>')
        self.write('<a href="#cluster">Cluster</a> &bull; ')
        self.write('<a href="#scheduler">Scheduler</a> &bull; ')
        self.write('<a href="#workers">Workers</a> &bull; ')
        self.write('<a href="#nannies">Nannies</a>')
        self.write('<a href="javascript:reload();" id="reload">↺</a>')
        self.write("</p></div>")

        self.write(
            '<button type="button" class="collapsible" id="cluster">Cluster</button><div class="content">'
        )
        self.write("<hr><table><tr><td>Level</td><td>Message</td></tr>")
        for log_text in cluster_logs.split("\n"):
            self.write(
                f"""<tr>
                        <td>-</td>
                        <td>{log_text}</td>
                    </tr>"""
            )
        self.write("</table><hr></div>")

        self.write(
            '<button type="button" class="collapsible" id="scheduler">Scheduler</button><div class="content">'
        )
        self.write("<hr><table><tr><td>Level</td><td>Message</td></tr>")
        for level, log_text in scheduler_logs:
            self.write(
                f"""<tr>
                        <td>{level}</td>
                        <td>{log_text}</td>
                    </tr>"""
            )
        self.write("</table><hr></div>")

        self.write(
            '<button type="button" class="collapsible" id="workers">Workers</button><div class="content">'
        )
        self.write(f"<h5>#workers: {len(worker_logs)}</h5>")
        for worker_num, (worker_addr, logs) in enumerate(worker_logs.items()):
            self.write(f"<h3>Worker[{worker_num}]-> {worker_addr}</h3><hr>")
            self.write("<table><tr><td>Level</td><td>Message</td></tr>")
            for level, log_text in logs:
                self.write(
                    f"""<tr>
                            <td>{level}</td>
                            <td>{log_text}</td>
                        </tr>"""
                )
            self.write("</table><hr>")
        self.write("</div>")

        self.write(
            '<button type="button" class="collapsible" id="nannies">Nannies</button><div class="content">'
        )
        self.write(f"<h5>#nannies: {len(nanny_logs)}</h5>")
        for nanny_num, (nanny_addr, logs) in enumerate(nanny_logs.items()):
            self.write(f"<h3>Nanny[{nanny_num}]-> {nanny_addr}</h3><hr>")
            self.write("<table><tr><td>Level</td><td>Message</td></tr>")
            for level, log_text in logs:
                self.write(
                    f"""<tr>
                            <td>{level}</td>
                            <td>{log_text}</td>
                        </tr>"""
                )
            self.write("</table><hr>")
        self.write("</div>")
        self.write("</body>")
        self.write(
            """<script>
    var coll = document.getElementsByClassName("collapsible");
    var i;

    for (i = 0; i < coll.length; i++) {
    coll[i].addEventListener("click", function() {
        this.classList.toggle("active");
        var content = this.nextElementSibling;
        if (content.style.display === "block") {
        content.style.display = "none";
        } else {
        content.style.display = "block";
        }
    });
    }

    window.onscroll = function() {myFunction()};

    var header = document.getElementById("myHeader");
    var sticky = header.offsetTop;

    function myFunction() {
    if (window.pageYOffset > sticky) {
        header.classList.add("sticky");
    } else {
        header.classList.remove("sticky");
    }
    }

    var origin_location = window.location.href;
    function reload() {
        window.location.href = origin_location;
    }
    </script>"""
        )
        self.write("</html>")


class AdaptHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        minimum = self.get_argument("minimumJobs")
        maximum = self.get_argument("maximumJobs")
        if minimum.lower() != "none":
            minimum = int(minimum)
        else:
            minimum = None
        if maximum.lower() != "none":
            maximum = int(maximum)
        else:
            maximum = None
        logger.debug(
            f"[AdaptHandler][send][op: adapt][minimum_jobs: {minimum}][maximum_jobs: {maximum}]"
        )
        self.sched_q.put(
            {"op": "adapt", "minimum_jobs": minimum, "maximum_jobs": maximum}
        )
        self.write(f"adapt jobs to min {minimum} and max {maximum}")

    def prepare(self):
        logger.debug(self.request.arguments)


class ScaleJobHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        num_jobs = int(self.get_argument("num"))
        if num_jobs < 0:
            self.write(f"abort scaling: {num_jobs}")
        else:
            logger.debug(f"[ScaleJobHandler][send][op: scale_jobs][num: {num_jobs}]")
            self.sched_q.put({"op": "scale_jobs", "num": num_jobs})
            self.write(f"scaled jobs to: {num_jobs}")

    def prepare(self):
        logger.debug(self.request.arguments)


class SchedulerIDHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        logger.debug("[SchedulerIDHandler][send][op: sched_id]")
        self.sched_q.put({"op": "sched_id"})
        res = self.controller_q.get()
        logger.debug(f"[SchedulerIDHandler][res: {res}]")
        self.write(res)


class ScaleWorkerHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        num_workers = int(self.get_argument("num"))
        if num_workers < 0:
            self.write(f"abort scaling: {num_workers}")
        else:
            logger.debug(
                f"[ScaleWorkerHandler][send][op: scale_workers][num: {num_workers}]"
            )
            self.sched_q.put({"op": "scale_workers", "num": num_workers})
            self.write(f"scaled workers to: {num_workers}")

    def prepare(self):
        logger.debug(self.request.arguments)


class WorkerSpecHandler(tornado.web.RequestHandler):
    def initialize(self, sched_q: Queue, controller_q: Queue):
        self.sched_q: Queue = sched_q
        self.controller_q: Queue = controller_q

    def get(self):
        """Return a descriptive dictionary of worker specs.

        Example worker_spec:
            {
                "HTCondorCluster-0": {
                    "cls": "__main__.MyHTCondorJob",
                    "options": {
                        "cores": 1,
                        "memory": "3 GB",
                        "disk": "1 GB",
                        "job_extra": {
                            "+OWNER": "condor",
                            "log": "simple.log",
                            "output": "simple.out",
                            "error": "simple.error"
                        },
                        "config_name": "htcondor",
                        "interface": "None",
                        "protocol": "tcp://",
                        "security": "None"
                    }
                },
                "HTCondorCluster-1": {
                    "cls": "__main__.MyHTCondorJob",
                    "options": {
                        "cores": 1,
                        "memory": "3 GB",
                        "disk": "1 GB",
                        "job_extra": {
                            "+OWNER": "condor",
                            "log": "simple.log",
                            "output": "simple.out",
                            "error": "simple.error"
                        },
                        "config_name": "htcondor",
                        "interface": "None",
                        "protocol": "tcp://",
                        "security": "None"
                    }
                }
            }"""
        logger.debug("[WorkerSpecHandler][send][op: worker_spec]")
        self.sched_q.put({"op": "worker_spec"})
        res = self.controller_q.get()
        logger.debug(f"[WorkerSpecHandler][res: {res}]")

        if res == "NOTMODIFIED":
            self.set_status(304)
        else:
            self.write(res)


def make_app(sched_q: Queue, controller_q: Queue):
    """Create scheduler support app"""
    return tornado.web.Application(
        [
            (r"/", MainHandler),
            (
                r"/jobScript",
                JobScriptHandler,
                dict(sched_q=sched_q, controller_q=controller_q),
            ),
            (r"/adapt", AdaptHandler, dict(sched_q=sched_q, controller_q=controller_q)),
            (
                r"/jobs",
                ScaleJobHandler,
                dict(sched_q=sched_q, controller_q=controller_q),
            ),
            (
                r"/workers",
                ScaleWorkerHandler,
                dict(sched_q=sched_q, controller_q=controller_q),
            ),
            (
                r"/workerSpec",
                WorkerSpecHandler,
                dict(sched_q=sched_q, controller_q=controller_q),
            ),
            (
                r"/schedulerID",
                SchedulerIDHandler,
                dict(sched_q=sched_q, controller_q=controller_q),
            ),
            (r"/close", CloseHandler, dict(sched_q=sched_q, controller_q=controller_q)),
            (
                r"/scaleZeroAndClose",
                ScaleZeroAndCloseHandler,
                dict(sched_q=sched_q, controller_q=controller_q),
            ),
            (r"/logs", LogsHandler, dict(sched_q=sched_q, controller_q=controller_q)),
        ],
        debug=True,
    )


async def main(sched_q: Queue, controller_q: Queue):
    loop = asyncio.get_running_loop()

    loop.create_task(start_controller(sched_q, controller_q))
    loop.create_task(tunnel_scheduler())
    loop.create_task(tunnel_dashboard())
    loop.create_task(tunnel_controller())

    running = True

    while running:
        await asyncio.sleep(14)
        logging.debug("Controller is Running...")
        # sched_q.put({"op": "job_script"})
        # sched_q.put({"op": "close"})
        # break


if __name__ == "__main__":
    logger.debug("create queues")
    sched_q: Queue = Queue()
    controller_q: Queue = Queue()

    logger.debug("create sched proc")
    sched_proc = SchedulerProc(sched_q, controller_q)
    logger.debug("start sched proc")
    sched_proc.start()
    res = controller_q.get()
    logger.debug(f"sched proc res: {res}")
    if res == "READY":
        logger.debug("!!!!! sched proc ready !!!!!")

    logger.debug("!!! start main loop !!!")
    asyncio.run(main(sched_q, controller_q))
