# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import asyncio
import logging
import os
import sys
import time
from signal import SIGTERM, signal

import asyncssh  # import now to avoid adding to module startup time
import dask
import tornado.ioloop
import tornado.web
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob

# dask.config.set({"distributed.worker.memory.spill": False})
# dask.config.set({"distributed.worker.memory.target": False})
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class MyHTCondorJob(HTCondorJob):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            python="source /cvmfs/cms.dodas.infn.it/miniconda3/etc/profile.d/conda.sh; conda activate cms-dodas; source /cvmfs/cms.dodas.infn.it/miniconda3/envs/cms-dodas/bin/thisroot.sh; export LD_LIBRARY_PATH=/cvmfs/cms.dodas.infn.it/miniconda3/envs/cms-dodas/lib:$LD_LIBRARY_PATH; python3",
        )
        self.submit_command = "./job_submit.sh"
        self.executable = "/bin/bash"


# JHUB_TOKEN={{ token }},JHUB_USER={{ name }},SCHED_PORT={{ sched_port }},DASH_PORT={{ dash_port }},
# RT={{ refresh_token }},IAM_SERVER={{ iam_server }},IAM_CLIENT_ID={{ client_id }},IAM_CLIENT_SECRET={{ client_secret }}
# _condor_AUTH_SSL_CLIENT_CAFILE={{ htc_ca }};_condor_TOOL_DEBUG={{ htc_debug }};_condor_COLLECTOR_HOST={{ htc_collector }}; _condor_SCHEDD_HOST={{ htc_schedd_host }};_condor_SCHEDD_NAME={{ htc_schedd_name }};_condor_SCITOKENS_FILE={{ htc_scitoken_file }};_condor_SEC_DEFAULT_AUTHENTICATION_METHODS={{ htc_sec_method}}

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
name = os.environ.get("JHUB_USER")
sched_port = int(os.environ.get("SCHED_PORT", "42000"))
dash_port = int(os.environ.get("DASH_PORT", "42001"))
tornado_port = int(os.environ.get("TORNADO_PORT", "42002"))

cluster = HTCondorCluster(
    job_cls=MyHTCondorJob,
    cores=1,
    memory="3 GB",
    disk="1 GB",
    scheduler_options={
        "host": ":{}".format(sched_port),
        "dashboard_address": "127.0.0.1:{}".format(dash_port),
    },
    job_extra={
        "+OWNER": '"condor"',
        "log": "simple.log",
        "output": "simple.out",
        "error": "simple.error",
    },
    silence_logs="debug",
)


# adapt = cluster.adapt(minimum_jobs=1, maximum_jobs=8)
# cluster.scale(jobs=1)


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
    # tornado.ioloop.IOLoop.current().start()


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(
            "Hello, may I help you? I guess not, you're in the wrong place... 6u"
        )


class CloseHandler(tornado.web.RequestHandler):
    def get(self):
        cluster.close()
        loop = asyncio.get_running_loop()
        loop.stop()


class ScaleJobHandler(tornado.web.RequestHandler):
    def get(self):
        num_jobs = self.get_argument("num")
        cluster.scale(jobs=num_jobs)
        self.write(f"scaled jobs to: {num_jobs}")

    def prepare(self):
        logger.debug(self.request.arguments)


class ScaleWorkerHandler(tornado.web.RequestHandler):
    def get(self):
        num_workers = self.get_argument("num")
        cluster.scale(n=num_workers)
        self.write(f"scaled workers to: {num_workers}")

    def prepare(self):
        logger.debug(self.request.arguments)


def make_app():
    return tornado.web.Application(
        [
            (r"/", MainHandler),
            (r"/jobs", ScaleJobHandler),
            (r"/workers", ScaleWorkerHandler),
            (r"/close", CloseHandler),
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
