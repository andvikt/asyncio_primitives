import pytest

import asyncio_primitives.primitives
from asyncio_primitives import wait_started, utils
import asyncio

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize('n_tasks', [1, 8])
async def test_custom_condition(n_tasks):

    cond1 = asyncio_primitives.primitives.CustomCondition()
    cond2 = asyncio_primitives.primitives.CustomCondition()

    hitcnt = 0

    async def hello():
        nonlocal hitcnt
        while True:
            async with utils.wait_for_any(cond1, cond2) as al:
                await al
                hitcnt+=1
                await asyncio.sleep(1)


    tasks = [asyncio.create_task(hello()) for x in range(n_tasks)]

    await asyncio.sleep(0.1)
    async with cond1:
        await cond1.notify_all()
    # we need to wait some time for new while-cycle to start
    await asyncio.sleep(0.1)
    async with cond2:
        await cond2.notify_all()
    assert hitcnt==n_tasks * 2

    for x in range(n_tasks):
        tasks[x].cancel()


@pytest.mark.parametrize('n_tasks', [1, 8])
@pytest.mark.timeout(1.3)
async def test_custom_check(n_tasks):

    cond1 = asyncio_primitives.primitives.CustomCondition()
    cond2 = asyncio_primitives.primitives.CustomCondition()

    hitcnt = 0
    g = 0

    async def hello():
        nonlocal hitcnt, g
        async with utils.wait_for_any(cond1, cond2, check=lambda: g == 5) as fut:
            await fut
            hitcnt+=1
            await asyncio.sleep(1)


    tasks = [asyncio.create_task(hello()) for x in range(n_tasks)]

    await asyncio.sleep(0.1)
    async with cond1:
        await cond1.notify_all()
    await asyncio.sleep(0.1)
    g = 5
    async with cond2:
        await cond2.notify_all()

    assert hitcnt == n_tasks

    for x in range(n_tasks):
        tasks[x].cancel()


async def test_endless_loop():

    cond1 = asyncio_primitives.primitives.CustomCondition()
    cond2 = asyncio_primitives.primitives.CustomCondition()

    hitcnt = 0
    g = 0

    @utils.endless_loop
    async def hello(started):
        nonlocal hitcnt, g
        async with utils.wait_for_any(cond1, cond2) as cond:
            started.set()
            await cond
            if g!=2:
                return
            hitcnt += 1
            await asyncio.sleep(1)

    # endless loop - декоратор, который возвращает корутину контекст
    # при входе в контекст проверяется, запущена корутина или нет
    # контекст отдает текущую задачу, которую можно отменить

    async with hello() as task:
        async with cond1:
            await cond1.notify_all()

    async with hello() as task:
        async with cond2:
            await cond2.notify_all()

    async with hello() as task:
        async with cond2:
            g = 2
            await cond2.notify_all()

    assert hitcnt == 1
    task.cancel()
    return


async def test_rule():


    cond1 = asyncio_primitives.primitives.CustomCondition()
    cond2 = asyncio_primitives.primitives.CustomCondition()

    hitcnt = 0
    g = 0

    @utils.rule(cond1, cond2, check=lambda:g==2)
    async def hello():
        nonlocal hitcnt
        hitcnt += 1
        await asyncio.sleep(1)


    async with hello() as task:
        async with cond1:
            await cond1.notify_all()

    async with hello() as task:
        await cond2.fast_notify()

    async with hello() as task:
        g = 2
        await cond2.fast_notify()


    assert hitcnt == 1

    await utils.notify_many(cond1, cond2)
    assert hitcnt == 2

    async with hello():
        raise utils.Continue()

    async with hello() as task:
        task.cancel()

    with pytest.raises(RuntimeError):
        await asyncio.sleep(0)
        async with hello() as task:
            pass

    return