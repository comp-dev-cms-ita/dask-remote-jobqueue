# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import weakref
from dask_jobqueue import HTCondorCluster

from distributed.deploy.spec import ProcessInterface
from dask.core import Job
import htcondor


class Process(ProcessInterface):
    """A superclass for SSH Workers and Nannies
    See Also
    --------
    Scheduler
    """

    def __init__(self, **kwargs):
        self.connection = None
        self.proc = None
        super().__init__(**kwargs)

    async def start(self):
        assert self.connection
        weakref.finalize(
            self, self.proc.kill
        )  # https://github.com/ronf/asyncssh/issues/112
        await super().start()

    async def close(self):
        # self.proc.kill()  # https://github.com/ronf/asyncssh/issues/112
        # self.connection.close()
        await super().close()

    def __repr__(self):
        return f"<SSH {type(self).__name__}: status={self.status}>"


class Scheduler(Process):
    """A Remote Dask Scheduler controlled via HTCondor
    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    connect_options: dict
        kwargs to be passed to asyncssh connections
    remote_python: str
        Path to Python on remote node to run this scheduler.
    kwargs: dict
        These will be passed through the dask-scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(
        self, address: str, connect_options: dict, kwargs: dict, remote_python=None
    ):
        super().__init__()

    async def start(self):
        # import asyncssh  # import now to avoid adding to module startup time

        # logger.debug("Created Scheduler Connection")

        # self.connection = await asyncssh.connect(self.address, **self.connect_options)

        # result = await self.connection.run("uname")
        # if result.exit_status == 0:
        #     set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
        #         dask.config.serialize(dask.config.global_config)
        #     )
        # else:
        #     result = await self.connection.run("cmd /c ver")
        #     if result.exit_status == 0:
        #         set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
        #             dask.config.serialize(dask.config.global_config)
        #         )
        #     else:
        #         raise Exception(
        #             "Scheduler failed to set DASK_INTERNAL_INHERIT_CONFIG variable "
        #         )

        # if not self.remote_python:
        #     self.remote_python = sys.executable

        # cmd = " ".join(
        #     [
        #         set_env,
        #         self.remote_python,
        #         "-m",
        #         "distributed.cli.dask_scheduler",
        #     ]
        #     + cli_keywords(self.kwargs, cls=_Scheduler)
        # )
        # self.proc = await self.connection.create_process(cmd)

        # # We watch stderr in order to get the address, then we return
        # while True:
        #     line = await self.proc.stderr.readline()
        #     if not line.strip():
        #         raise Exception("Worker failed to start")
        #     logger.info(line.strip())
        #     if "Scheduler at" in line:
        #         self.address = line.split("Scheduler at:")[1].strip()
        #         break
        # logger.debug("%s", line)
        await super().start()

    async def close(self):
        # self.proc.kill()  # https://github.com/ronf/asyncssh/issues/112
        # self.connection.close()
        await super().close()


class RemoteHTCondorCluster(HTCondorCluster):
    def __init__(
        self,
        n_workers=0,
        job_cls: Job = None,
        # Cluster keywords
        loop=None,
        security=None,
        silence_logs="error",
        name=None,
        asynchronous=False,
        # Scheduler-only keywords
        dashboard_address=None,
        host=None,
        scheduler_options=None,
        # Options for both scheduler and workers
        interface=None,
        protocol="tcp://",
        # Job keywords
        config_name=None,
        **job_kwargs,
    ):
        scheduler = {
            "cls": Scheduler,  # Use local scheduler for now
            "options": scheduler_options,
        }

        super().__init__(
            scheduler=scheduler,
        )
