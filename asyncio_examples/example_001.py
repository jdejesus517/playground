"""Simultaneous in/out streaming reference example."""

import asyncio
import dataclasses
import logging
import random
from asyncio import Event, Queue
from typing import AsyncGenerator, AsyncIterable

logging.basicConfig(level=logging.DEBUG)

_log = logging.getLogger(__name__)


@dataclasses.dataclass
class InputItem:
    """The item or object produced from a remote stream."""

    item_number: int

    def __str__(self):
        """Return a human-readable name."""
        return f"InputItem.{self.item_number}"


@dataclasses.dataclass
class OutputItem:
    """The item or object the RPC is supposed to generate."""

    input_item_number: int
    output_item_number: int

    def __str__(self):
        """Return a human-readable name."""
        return f"OutputItem.{self.input_item_number}.{self.output_item_number}"


async def mock_async_stream() -> AsyncIterable[OutputItem]:
    """Mock the behavior of an async stream."""
    for oid in range(1, 11):
        # simulate varying times to receive the next message
        await asyncio.sleep(random.uniform(0.2, 0.7))
        yield InputItem(item_number=oid)


async def rpc_function() -> AsyncGenerator[OutputItem, None]:
    """Mock RPC function."""
    max_worker_count = 5
    input_queue: Queue[InputItem] = Queue()

    # this event will allow workers to know when they should complete as there
    # will be no more items entering the queue
    fill_end_event: Event = Event()

    output_queue: Queue[OutputItem] = Queue()

    # get the stream iterator ready for consumption by the filler task
    stream = mock_async_stream()

    # kick off the filler task so that the input queue may start receiving items
    filler_task = asyncio.create_task(
        filler_function(stream, input_queue, fill_end_event)
    )

    # store the workers in a set() to maintain there references
    workers = {
        asyncio.create_task(
            worker_function(input_queue, output_queue, fill_end_event),
            name=f"Worker[{worker_id}]",
        )
        for worker_id in range(1, max_worker_count + 1)
    }

    # have tasks clean themselves up when they are complete
    for worker in workers:
        worker.add_done_callback(workers.discard)

    count = 0
    # while there are output items that can be yielded or active workers that are still producing
    # output items, we wait for an item from the queue
    while output_queue.qsize() > 0 or len(workers) > 0:
        _log.debug("Active workers: %d", len(workers))
        _log.debug("Yield count: %d", count)
        try:
            # it is possible that we might try and pull from the output queue when there will never be anymore
            # output items but there was 1 worker still alive because we got here before it could clean
            # itself up. this timeout lets us move on while the remaining worker produces more items or
            # cleans itself up.
            item = await asyncio.wait_for(output_queue.get(), timeout=0.05)
            yield item
            count += 1
        except asyncio.exceptions.TimeoutError:
            continue

    _log.debug("Total items yielded: %d", count)


async def worker_function(
    input_queue: Queue[InputItem],
    output_queue: Queue[OutputItem],
    fill_end_event: Event,
) -> None:
    """Execute the processing function until all items are processed.

    :param input_queue: the input queue
    :param output_queue: the output queue
    :param fill_end_event: an event to alert when filling has stopped
    :return: None
    """
    # this worker should run while there are items in the queue or if there is an expectation that
    # more items will be coming
    while input_queue.qsize() > 0 or not fill_end_event.is_set():
        input_item = await input_queue.get()
        await processing_function(input_item, output_queue)


async def processing_function(
    input_item: InputItem, output_queue: Queue[OutputItem]
) -> None:
    """Process the input item to generate output items.

    :param input_item: an input item
    :param output_queue: the queue place completed output items into
    :return: None
    """
    for oid in range(1, 11):
        # simulate varying times to generate the next output message
        await asyncio.sleep(random.uniform(0.1, 0.4))
        # add an item to the output queue so it can be read and yielded while items are still processing
        await output_queue.put(
            OutputItem(input_item_number=input_item.item_number, output_item_number=oid)
        )


async def filler_function(
    input_stream: AsyncIterable, input_queue: Queue[InputItem], fill_end_event: Event
) -> None:
    """Fill an input queue with the input items fetch from a streaming source.

    :param input_stream: a streaming source
    :param input_queue: a queue to place the input items into
    :param fill_end_event: an event to flag when filling is complete
    :return: None
    """
    async for item in input_stream:
        await input_queue.put(item)

    _log.debug("Filling completed. No more items incoming.")
    fill_end_event.set()


async def main():
    """Run the main program."""
    async for item in rpc_function():
        _log.info("%s", item)

    _log.info("Program Completed.")


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
