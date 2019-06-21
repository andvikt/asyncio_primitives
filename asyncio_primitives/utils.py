import typing
import asyncio
from logging import getLogger
from contextlib import asynccontextmanager, AbstractAsyncContextManager
from collections import deque
from dataclasses import dataclass, field

logger = getLogger('async_run')
_T = typing.TypeVar('_T')

ASYNC_RUN_FOO = typing.Union[
            typing.Awaitable[_T]
            , typing.Callable[[], typing.Union[_T, typing.Awaitable[_T]]]
        ]


async def async_run(foo: ASYNC_RUN_FOO, *args, **kwargs):
    """
    Helps run async functions, coroutines, sync-callable in a single manner
    :param foo:
    :param args: will be passed to foo if it is function
    :param kwargs: will be passed to foo if it is function
    :return:
    """
    if asyncio.iscoroutinefunction(foo):
        return await foo(*args, **kwargs)
    elif asyncio.iscoroutine(foo):
        return await foo
    elif isinstance(foo, typing.Callable):
        return foo(*args, **kwargs)
    else:
        raise TypeError(f'{foo} is not callable or awaitable')


async def wait_started(foo: ASYNC_RUN_FOO
                       , cancel_callback: ASYNC_RUN_FOO = None
                       ) -> asyncio.Task:
    """
    Helps to run some foo as a task and wait for started
    :param foo: some async function or coroutine
    :param cancel_callback:
    :return:
    """
    started = asyncio.Event()

    async def start():
        started.set()

    async def wrap():
        try:
            await asyncio.gather(async_run(foo), start())
        except asyncio.CancelledError:
            logger.debug(f'{foo} cancelled')
            raise
        except Exception as err:
            raise err
        finally:
            if cancel_callback:
                await async_run(cancel_callback)

    ret = asyncio.create_task(wrap())
    await started.wait()
    await asyncio.sleep(0)
    return mark(ret, markers=['_for_cancel'])


def mark(foo=None, *, markers: typing.Iterable[str]):
    """
    Add some markers to foo
    :param foo: any object
    :param args: some str markers
    :return:
    """
    def deco(_foo):
        for x in markers:
            try:
                setattr(_foo, x, True)
            except AttributeError:
                raise
        return _foo
    return deco(foo) if foo is not None else deco


async def cancel_all():
    for x in asyncio.all_tasks():
        if hasattr(x, '_for_cancel'):
            x.cancel()
            await x


@asynccontextmanager
async def wait_for_any(*conditions: asyncio.Condition, check=(lambda : True)):
    """
    Contextmanager, waits for any of the condition to happen, when happens, enters context, on context exit informs
    all the conditions that it has exited context

    :param conditions:
    :return:
    """

    from .primitives import CustomCondition

    async def add_to_waiters():
        c = iter(conditions)
        ftr = asyncio.Future()
        async def iterate():
            try:
                x = next(c)
                if x is None:
                    return
                async with x:
                    x._waiters.append(ftr)
                    if isinstance(x, CustomCondition):
                        x.exits.append(asyncio.Future())
                    await iterate()
            except StopIteration:
                pass
        await iterate()
        return ftr

    async def exit(ftr):
        for x in conditions:
            if ftr in x._waiters:
                x._waiters.remove(ftr)
            if isinstance(x, CustomCondition):
                ftr = x.exits.popleft()
                ftr.set_result(True)

    ftr = await add_to_waiters()

    async def wait():
        nonlocal ftr
        await ftr
        while not check():
            await exit(ftr)
            await asyncio.sleep(0)
            ftr = await add_to_waiters()
            await ftr
    try:
        yield wait()
    finally:
        if not ftr.done():
            raise RuntimeError(f'Condition was not awaiten')
        await exit(ftr)




def endless_loop(foo):
    """
    Decorator. Used to wrap a foo into endless loop, start it immediate
    Returns context manager, that exits context only when the next loop cycle finishes
    :return:
    """
    started = asyncio.Event()
    waiters: typing.Deque[asyncio.Future] = deque()

    async def start():
        while True:
            try:
                await foo(started)
            finally:
                for x in waiters:
                    x.set_result(True)
                started.clear()

    @asynccontextmanager
    async def context() -> asyncio.Task:
        await started.wait()
        ftr = asyncio.Future()
        waiters.append(ftr)
        try:
            yield task
            await ftr
        finally:
            waiters.remove(ftr)

    task = asyncio.create_task(start())

    return context


def rule(*conditions, check=lambda: True):

    def deco(foo):

        @endless_loop
        async def hello(started):
            async with wait_for_any(*conditions) as cond:
                started.set()
                await cond
                if not check():
                    return
                await async_run(foo)

        return hello

    return deco