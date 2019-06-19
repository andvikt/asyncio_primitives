import typing
import asyncio
from logging import getLogger


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
        except Exception as err:
            raise err
        finally:
            if cancel_callback:
                await async_run(cancel_callback)

    ret = asyncio.create_task(wrap())
    await started.wait()
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