import asyncio
from multiprocessing import Process, Queue

import asyncssh
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
