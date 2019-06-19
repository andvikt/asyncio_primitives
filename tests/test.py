import pytest
from asyncio_primitives import ConditionRunner, AsyncContextCounter, wait_started, ConditionAsyncNotify
import asyncio

pytestmark = pytest.mark.asyncio


async def test_counter():

    counter = AsyncContextCounter()
    res = []

    async def hello():
        async with counter:
            res.append(counter.idx)
            await asyncio.sleep(1)
        res.append(counter.idx)

    await wait_started(asyncio.gather(*[hello() for x in range(4)]))
    await counter.wait()
    res.append(10)
    await asyncio.sleep(0)
    assert res == [1, 2, 3, 4, 10, 0, 0, 0, 0]

async def test_custom_condition():

    count = ConditionRunner()
    chck = []

    async def test(idx):
        nonlocal chck
        chck.append(idx)

    async def race(start):
        for x in range(4):
            async with count as que:
                await que.put(test(start + x))


    await asyncio.gather(*[race(j*4) for j in range(4)])

    await count.notify_all()
    print(chck)
    assert chck == [0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15]


async def test_async_notify():

    cond = ConditionAsyncNotify()
    chk = []

    async def hello(j):
        async with cond:
            chk.append(j)
        chk.append(j)

    for x in range(4):
        await wait_started(hello(x))

    await cond.notify_all()
    chk.append(10)
    assert chk == [0, 1, 2, 3, 10]
    await asyncio.sleep(0)
    assert chk == [0, 1, 2, 3, 10, 0, 1, 2, 3]
