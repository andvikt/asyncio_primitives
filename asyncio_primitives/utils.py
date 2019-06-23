import typing
import asyncio
from logging import getLogger
from contextlib import asynccontextmanager, AbstractAsyncContextManager, contextmanager, AsyncExitStack
from collections import deque
from dataclasses import dataclass, field
from functools import wraps

import warnings

logger = getLogger('async_run')
_T = typing.TypeVar('_T')

ASYNC_RUN_FOO = typing.Union[
            typing.Awaitable[_T]
            , typing.Callable[[], typing.Union[_T, typing.Awaitable[_T]]]
        ]

class Continue(Exception):
    pass

class ErrInLoop(Warning):
    pass

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


async def wait_started(*foos: ASYNC_RUN_FOO, **kwargs) -> asyncio.Task:
    """
    Helps to run some foo as a task and wait for it to be started, foo must accept as a first argument started callback
    :param foo: some async function or coroutine
    :param cancel_callback:
    :return:
    """
    started = [asyncio.Future() for x in range(len(foos))]

    async def start(st):
        st.set_result(True)

    async def wrap():
        try:
            await asyncio.gather(*[async_run(foo, start(started[i]), **kwargs) for i, foo in enumerate(foos)])
        except asyncio.CancelledError:
            logger.debug(f'{foos} cancelled')
            raise

    ret = asyncio.create_task(wrap())
    await asyncio.gather(*started)
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


from .primitives import CustomCondition


@asynccontextmanager
async def wait_for_any(*conditions: CustomCondition):
    """
    Contextmanager, waits for any of the condition to happen, when happens, enters context, on context exit informs
    all the conditions that it has exited context

    :param conditions:
    :return:
    """

    ftr = asyncio.Future()
    ex = asyncio.Future()
    try:
        async with AsyncExitStack() as context:
            await asyncio.gather(*[context.enter_async_context(x) for x in conditions])
            for x in conditions:
                x._waiters.append(ftr)
                x.exits.append(ex)

        yield ftr

        if not (ftr.done() or ftr.cancelled()):
            raise RuntimeError('Future is not awaited')

    finally:
        ex.set_result(True)
        for x in conditions:
            if ftr in x._waiters:
                x._waiters.remove(ftr)


@contextmanager
def ignoreerror(*errors):
    try:
        yield
    except Exception as err:
        if err.__class__ not in errors:
            raise


def endless_loop(foo):
    """
    Decorate foo, when decorated foo is called, it starts a task, where foo is called in endless while-true loop
    Note, foo must be asyncfoo end it must take some time to act, if it is called more often them WARN_FAST_LOOP time,
    the warning will be raised
    When errors raised inside foo, they are ignored, only warning will be produced
    :param _foo:
    :return:
    """
    @wraps(foo)
    async def wrapper(*args, **kwargs) -> asyncio.Task:
        async def start(started):
            await started
            while True:
                try:
                    await foo(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    warnings.warn(ErrInLoop(f'Error in endless loop, continue\n{err}'))
                    await asyncio.sleep(0)

        return await wait_started(start)
    return wrapper


def rule(*conditions, check=lambda: True):

    def deco(foo):

        @wraps(foo)
        async def wrapper(*args, **kwargs):
            @endless_loop
            async def run():
                async with wait_for_any(*conditions) as cond:
                    await cond
                    if not check():
                        return
                    await async_run(foo, *args, **kwargs)

            return await run()

        return wrapper

    return deco


async def notify_many(*conditions: CustomCondition):

    async with AsyncExitStack() as stack:
        await asyncio.gather(*[stack.enter_async_context(x) for x in conditions])
        await asyncio.gather(*[x.notify_all() for x in conditions])
