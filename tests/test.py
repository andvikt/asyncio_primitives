from logging import basicConfig, DEBUG
basicConfig(level=DEBUG)
import pytest
import asyncio_primitives.primitives
from asyncio_primitives import wait_started, utils
import asyncio
import random

pytestmark = pytest.mark.asyncio


async def cancel(task):
    with pytest.raises(asyncio.CancelledError):
        task.cancel()
        await task



@pytest.mark.parametrize('n_tasks', [1, 100])
@pytest.mark.timeout(1.6)
async def test_custom_condition(n_tasks):


    cond1 = asyncio_primitives.primitives.CustomCondition()
    cond2 = asyncio_primitives.primitives.CustomCondition()

    hitcnt = 0

    @utils.set_name('test name')
    async def hello(started):
        nonlocal hitcnt
        await started
        while True:
            async with utils.wait_for_any(cond1, cond2) as al:
                await al
                hitcnt+=1
                await asyncio.sleep(random.random() * 0.5)

    task = await utils.wait_started(*[hello for x in range(n_tasks)])

    await cond1.fast_notify()
    await cond2.fast_notify()

    await utils.notify_many(cond1, cond2)

    assert hitcnt==n_tasks * 3

    with pytest.raises(asyncio.CancelledError):
        task.cancel()
        await task


@pytest.mark.parametrize('n_tasks', [1, 100])
@pytest.mark.timeout(0.7)
async def test_endless_loop(n_tasks):

    cond1 = asyncio_primitives.primitives.CustomCondition()
    cond2 = asyncio_primitives.primitives.CustomCondition()

    hitcnt = 0
    g = 0

    @utils.endless_loop
    async def hello(y, uhh):
        nonlocal hitcnt, g
        assert y == 1, uhh == 2
        async with utils.wait_for_any(cond1, cond2) as cond:
            await cond
            if g!=2:
                return
            hitcnt += 1
            await asyncio.sleep(0.25)

    assert utils.is_starter(hello)
    tasks = await asyncio.gather(*[hello(1, uhh=2) for i in range(n_tasks)])

    await cond2.fast_notify()
    assert hitcnt == 0
    g = 2
    await asyncio.gather(*[cond1.fast_notify() for i in range(4)])
    assert hitcnt == 1 * n_tasks
    await utils.notify_many(cond1, cond2)
    assert hitcnt == 2 * n_tasks

    await asyncio.gather(*[cancel(task) for task in tasks])


@pytest.mark.parametrize('n_tasks', [1, 100])
@pytest.mark.timeout(0.6)
async def test_rule(n_tasks):


    cond1 = asyncio_primitives.primitives.CustomCondition()
    cond2 = asyncio_primitives.primitives.CustomCondition()

    hitcnt = 0
    g = 0

    @utils.rule(cond1, cond2, check=lambda: g == 2)
    async def hello(u, y):
        nonlocal hitcnt
        assert u == 1, y == 2
        hitcnt += 1
        await asyncio.sleep(0.25)

    assert utils.is_starter(hello)
    tasks = await asyncio.gather(*[hello(1, y=2) for i in range(n_tasks)])

    await cond1.fast_notify()
    assert hitcnt == 0

    g = 2
    await cond2.fast_notify()
    assert hitcnt == 1 * n_tasks

    await utils.notify_many(cond1, cond2)
    assert hitcnt == 2 * n_tasks

    await asyncio.gather(*[cancel(task) for task in tasks])


async def test_errors():

    @utils.endless_loop
    async def hello():
        raise asyncio.CancelledError()


    task: asyncio.Task = await hello()

    with pytest.raises(asyncio.CancelledError):
        await task

    @utils.endless_loop
    async def hy():
        raise utils.Continue()

    task = await hy()
    with pytest.warns(utils.ErrInLoop):
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=0.5)

    await cancel(task)
