# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import asyncio
import json
import logging
import os

import asyncssh
import tornado.ioloop
import tornado.web
from dask.distributed import Client
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class MyHTCondorJob(HTCondorJob):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            python="source /cvmfs/cms.dodas.infn.it/miniconda3/etc/profile.d/conda.sh; conda activate cms-dodas; source /cvmfs/cms.dodas.infn.it/miniconda3/envs/cms-dodas/bin/thisroot.sh; python3",
        )
        self.submit_command = "./job_submit.sh"
        self.executable = "/bin/bash"


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

htc_ca = "$PWD/ca.crt"
# os.environ.get("_condor_AUTH_SSL_CLIENT_CAFILE")
htc_debug = os.environ.get("_condor_TOOL_DEBUG")
htc_collector = os.environ.get("_condor_COLLECTOR_HOST")
htc_schedd_host = os.environ.get("_condor_SCHEDD_HOST")
htc_schedd_name = os.environ.get("_condor_SCHEDD_NAME")
htc_scitoken_file = "$PWD/token"
# os.environ.get("_condor_SCITOKENS_FILE")
htc_sec_method = os.environ.get("_condor_SEC_DEFAULT_AUTHENTICATION_METHODS")
token = os.environ.get("JHUB_TOKEN")
name = os.environ.get("JHUB_USER", "")
sched_port = int(os.environ.get("SCHED_PORT", "42000"))
dash_port = int(os.environ.get("DASH_PORT", "42001"))
tornado_port = int(os.environ.get("TORNADO_PORT", "42002"))

site = None
machine_ad_file = os.environ.get("_CONDOR_MACHINE_AD", "")

if machine_ad_file:
    with open(machine_ad_file) as f:
        lines = f.readlines()
        for l in lines:
            if l.startswith("SiteName"):
                site = l.split("= ")[1].strip("\n").strip()
                break

logger.debug(f"SiteName is: {site}")

scheduler_options_vars = {
    "host": ":{}".format(sched_port),
    "dashboard_address": "127.0.0.1:{}".format(dash_port),
}
job_extra_vars = {
    "+OWNER": '"' + name.split("-")[0] + '"',
    "log": "wn.log",
    "output": "wn.out",
    "error": "wn.error",
    "should_transfer_files": "YES",
}

if site:
    job_extra_vars["requirements"] = f"( SiteName == {site} )"

cluster = HTCondorCluster(
    job_cls=MyHTCondorJob,
    cores=1,
    memory="3 GB",
    disk="1 GB",
    scheduler_options=scheduler_options_vars,
    job_extra=job_extra_vars,
    silence_logs="debug",
)

# Set the cluster to adaptiv mode, with min and max
# TODO: pass minumum and maximum by ENV VAR
cluster.adapt(minimum_jobs=0, maximum_jobs=16)


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
        "127.0.0.1", sched_port, "127.0.0.1", sched_port
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
        "127.0.0.1", dash_port, "127.0.0.1", dash_port
    )
    await forwarder.wait_closed()


async def tunnel_tornado():
    logger.debug("start tunnel tornado")
    connection = await asyncssh.connect(
        "jhub.131.154.96.124.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", tornado_port, "127.0.0.1", tornado_port
    )
    await forwarder.wait_closed()


async def start_tornado():
    logger.debug("start tornado web")
    app = make_app()
    app.listen(tornado_port, address="127.0.0.1")


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(
            "Hello, may I help you? I guess not, you're in the wrong place... 6u"
        )


class CloseHandler(tornado.web.RequestHandler):
    def get(self):
        cluster.close()
        self.write("cluster closed")


class LogsHandler(tornado.web.RequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self._client = Client(cluster)
        self._client = Client("tcp://127.0.0.1:57122", asynchronous=True)

    async def get(self):
        scheduler_logs: list[tuple] = await self._client.get_scheduler_logs()
        worker_logs: list[tuple] = await self._client.get_worker_logs()
        nanny_logs: list[tuple] = await self._client.get_worker_logs(nanny=True)
        self.write(
            """<!DOCTYPE html>
            <html>
            <head>
            <title>Logs</title>
            <meta http-equiv="refresh" content="30">
            </head>
            <style>
            table, th, td {
            border: 1px solid black;
            }

            table {
            width: 100%;
            }
            </style>
            <body>
            """
        )
        self.write("<p>")
        self.write('<a href="#scheduler">Scheduler</a> | ')
        self.write('<a href="#workers">Workers</a> | ')
        self.write('<a href="#nannies">Nannies</a>')
        self.write("</p>")
        self.write('<h1 id="scheduler">Scheduler</h1><hr>')
        self.write("<table><tr><td>Level</td><td>Message</td></tr>")
        for level, log_text in scheduler_logs:
            self.write(
                f"""<tr>
                        <td>{level}</td>
                        <td>{log_text}</td>
                    </tr>"""
            )
        self.write("</table>")
        self.write('<h1 id="workers">Workers</h1><hr>')
        for worker_addr, logs in worker_logs.items():
            self.write(f"<h3>{worker_addr}</h3><hr>")
            self.write("<table><tr><td>Level</td><td>Message</td></tr>")
            for level, log_text in logs:
                self.write(
                    f"""<tr>
                            <td>{level}</td>
                            <td>{log_text}</td>
                        </tr>"""
                )
            self.write("</table>")
        self.write('<h1 id="nannies">Nannies</h1><hr>')
        for nanny_addr, logs in nanny_logs.items():
            self.write(f"<h3>{nanny_addr}</h3><hr>")
            self.write("<table><tr><td>Level</td><td>Message</td></tr>")
            for level, log_text in logs:
                self.write(
                    f"""<tr>
                            <td>{level}</td>
                            <td>{log_text}</td>
                        </tr>"""
                )
            self.write("</table>")
        self.write("</body></html>")


class ScaleJobHandler(tornado.web.RequestHandler):
    def get(self):
        num_jobs = int(self.get_argument("num"))
        cluster.scale(jobs=num_jobs)
        self.write(f"scaled jobs to: {num_jobs}")

    def prepare(self):
        logger.debug(self.request.arguments)


class ScaleWorkerHandler(tornado.web.RequestHandler):
    def get(self):
        num_workers = int(self.get_argument("num"))
        cluster.scale(n=num_workers)
        self.write(f"scaled workers to: {num_workers}")

    def prepare(self):
        logger.debug(self.request.arguments)


class WorkerSpecHandler(tornado.web.RequestHandler):
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
        workers = {}
        for num, (_, spec) in enumerate(cluster.worker_spec.items()):
            memory_limit = spec["options"]["memory"].lower()
            if memory_limit.find("gb"):
                memory_limit = int(memory_limit.replace("gb", "").strip()) * 10 ** 9
            else:
                memory_limit = -1
            # See dask-labextension make_cluster_model
            workers[str(num)] = {
                "nthreads": spec["options"]["cores"],
                "memory_limit": memory_limit,
            }
        self.write(json.dumps(workers))


def make_app():
    """Create scheduler support app"""
    return tornado.web.Application(
        [
            (r"/", MainHandler),
            (r"/jobs", ScaleJobHandler),
            (r"/workers", ScaleWorkerHandler),
            (r"/workerSpec", WorkerSpecHandler),
            (r"/close", CloseHandler),
            (r"/logs", LogsHandler),
        ],
        debug=True,
    )


async def main():
    loop = asyncio.get_running_loop()
    loop.create_task(start_tornado())
    loop.create_task(tunnel_scheduler())
    loop.create_task(tunnel_dashboard())
    loop.create_task(tunnel_tornado())
    while True:
        logging.debug("running")
        await asyncio.sleep(60)


if __name__ == "__main__":
    logger.debug("start main loop")
    asyncio.run(main())
