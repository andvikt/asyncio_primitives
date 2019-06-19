import asyncio
from contextlib import AbstractAsyncContextManager
from . import utils


class AsyncContextCounter(AbstractAsyncContextManager):
    """
    Counts every enter to the context
    When exiting context, notify inbound Condition and decreasing counter
    """
    def __init__(self):
        self.cond = asyncio.Condition()
        self.inc_lock = asyncio.Lock()
        self.lck = asyncio.Lock()
        self.idx = 0

    async def acquire(self):
        async with self.inc_lock:
            async with self.lck:
                self.idx+=1

    async def release(self):
        async with self.lck:
            self.idx-=1
        async with self.cond:
            self.cond.notify_all()
        await asyncio.sleep(0)

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

    async def wait(self, idx=0):
        async with self.inc_lock:
            async with self.cond:
                await self.cond.wait_for(lambda : self.idx == idx)


class ConditionAsyncNotify(AbstractAsyncContextManager):
    """
    Condition with async notification.
    It uses AsyncContextCounter to count all waiters.
    When entering context, it acquires internal counter, then waits for notification, then enter context,
        then on exit release counter. So no need to await condition inside context, it is already awaited
    When call notify_all blocks until all waiters release their counter context.
    """
    def __init__(self):
        self.cond = asyncio.Condition()
        self.counter = AsyncContextCounter()

    async def acquire(self):
        await self.counter.acquire()
        async with self.cond:
            await self.cond.wait()

    async def release(self):
        await self.counter.release()

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

    async def notify_all(self):
        async with self.cond:
            self.cond.notify_all()
        await self.counter.wait()


class ConditionRunner(AbstractAsyncContextManager):
    """
    When we enter its context, we get que that is used to add coros.
    That coros will be called only when notify_all is called.
    Also we ensure that waiting coro is started before exiting context
    Note that notify_all blocks untill all the waiting coros will be done
    """
    def __init__(self, cond=None):
        self.cond = ConditionAsyncNotify()
        self.tasks = asyncio.Queue()

    async def __aenter__(self):
        return self.tasks

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        while True:
            try:
                task = self.tasks.get_nowait()
            except asyncio.QueueEmpty:
                break

            async def wrap():
                async with self.cond:
                    await task

            await utils.wait_started(wrap())

    async def notify_all(self):
        await self.cond.notify_all()

