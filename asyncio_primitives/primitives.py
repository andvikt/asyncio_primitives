import asyncio
from collections import deque
import typing


class CustomCondition(asyncio.Condition):

    """
    It has async notify_all.
    The goal is to wait until all current waiters exit their context
    Useful when we dont want to continue execution before all waiters finish their job
    """

    def __init__(self):
        super().__init__()
        self.exits: typing.Deque[asyncio.Future] = deque()

    async def __aenter__(self):
        ret = await super().__aenter__()
        return ret

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def notify_all(self) -> None:
        super().notify_all()
        await asyncio.gather(*list(self.exits))
        self.exits.clear()

    async def fast_notify(self):
        """
        Just a shorthand of
        async with self:
            await self.notify_all()
        :return:
        """
        async with self:
            await self.notify_all()
