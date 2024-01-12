from threading import Thread
import threading
from collections import deque
from loguru import logger
import os
import asyncio
import time
from rdtsctots import rdtsctotsOnline

from ul_online_gnb import ProcessULGNB
from ul_online_nrue import ProcessULUE
from ul_online_nlmt import process_ul_nlmt
from ul_online_combine import CombineUL
from ul_process_journeys import process_ul_journeys

MAX_L1_UPF_DEPTH = 1000 # lines
MAX_L2_UPF_DEPTH = 100 # journeys

MAX_L1_GNB_DEPTH = 1000 # lines
MAX_L2_GNB_DEPTH = 100 # journeys

MAX_L1_UE_DEPTH = 1000 # lines
MAX_L2_UE_DEPTH = 100 # journeys

class RingBuffer:
    def __init__(self, size):
        self.size = size
        self.buffer = deque(maxlen=size)
        self.lock = threading.Lock()

    def append(self, item):
        with self.lock:
            if len(self.buffer) == self.size:
                logger.warning("RingBuffer is being overwritten. Consider increasing the buffer size.")
            self.buffer.append(item)

    def get_items(self):
        with self.lock:
            return list(self.buffer)

    def reverse_items(self):
        with self.lock:
            return list(reversed(self.buffer))

    def pop_items(self, n):
        with self.lock:
            popped_items = []
            for _ in range(min(n, len(self.buffer))):
                popped_items.append(self.buffer.popleft())
            return popped_items
        
    def get_length(self):
        with self.lock:
            return len(self.buffer)


async def process_queues(upf_rawdata_queue, gnb_rawdata_queue, ue_rawdata_queue, config):
    upf_journeys_queue = RingBuffer(MAX_L2_UPF_DEPTH)
    gnb_journeys_queue = RingBuffer(MAX_L2_GNB_DEPTH)
    ue_journeys_queue = RingBuffer(MAX_L2_UE_DEPTH)
    gnbrdts = rdtsctotsOnline("GNB")
    gnbproc = ProcessULGNB()
    uerdts = rdtsctotsOnline("UE")
    ueproc = ProcessULUE()
    combineul = CombineUL()
    try:
        while True:
            await asyncio.sleep(0.1)
            upf_queue_length = upf_rawdata_queue.get_length()
            gnb_queue_length = gnb_rawdata_queue.get_length()
            ue_queue_length = ue_rawdata_queue.get_length()
            
            #logger.info(f"Queue lengths: UPF={upf_queue_length}, GNB={gnb_queue_length}, UE={ue_queue_length}")

            # NLMT
            if upf_queue_length > 0:
                lines = upf_rawdata_queue.pop_items(upf_queue_length)
                nlmt_journeys = process_ul_nlmt(lines)
                for journey in nlmt_journeys:
                    upf_journeys_queue.append(journey)

            # GNB
            if gnb_queue_length > 500:
                lines = gnb_rawdata_queue.pop_items(gnb_queue_length)
                l1linesgnb = gnbrdts.return_rdtsctots(lines)
                if len(l1linesgnb) > 0:
                    gnb_journeys = gnbproc.run(l1linesgnb)
                    for journey in gnb_journeys:
                        gnb_journeys_queue.append(journey)

            # UE
            if ue_queue_length > 500:
                lines = ue_rawdata_queue.pop_items(ue_queue_length)
                l1linesue = uerdts.return_rdtsctots(lines)
                l1linesue.reverse()
                if len(l1linesue) > 0:
                    ue_journeys = ueproc.run(l1linesue)
                    for journey in ue_journeys:
                        ue_journeys_queue.append(journey)
    
            # Create e2e jouneys
            ue_jrny_len = ue_journeys_queue.get_length()
            gnb_jrny_len = gnb_journeys_queue.get_length()
            upf_jrny_len = upf_journeys_queue.get_length()
            if ue_jrny_len > 20 and gnb_jrny_len > 20 and upf_jrny_len > 20:
                df = combineul.run(
                    upf_journeys_queue.pop_items(upf_jrny_len),
                    gnb_journeys_queue.pop_items(gnb_jrny_len),
                    ue_journeys_queue.pop_items(ue_jrny_len)
                )
                df = process_ul_journeys(df)
                print(df)

    except asyncio.CancelledError:
        pass

async def handle_client(reader, writer, client_name, config, rawdata_queue):
    init = True
    rem_str = ''
    try:
        while True:
            data = await reader.read(512)
            if not data:
                break
            if init:
                addr = writer.get_extra_info('peername')
                logger.info(f"[{client_name} server] connection from {addr}.")
                init = False
            message = data.decode()
            if message[-1] == '\n':
                received_lines = message.splitlines()
                if rem_str != '':
                    received_lines[0] = rem_str + received_lines[0]
                    rem_str = ''
                for line in received_lines:
                        rawdata_queue.append(line)
            else:
                if '\n' in message:
                    received_lines = message.splitlines()
                    received_lines[0] = rem_str + received_lines[0]
                    rem_str = ''
                    for line in received_lines[:-1]:
                            rawdata_queue.append(line)
                    rem_str = received_lines[-1]
                else:
                    rem_str = rem_str + message
            
    except asyncio.CancelledError:
        pass
    finally:
        logger.warning(f"[{client_name} server] Closing the connection")
        writer.close()

async def start_server(client_name, config, rawdata_queue):
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, client_name, config, rawdata_queue), 
        host='0.0.0.0', 
        port=config[client_name]["PORT"]
    )
    
    logger.info(f'[{client_name} server] serving on {server.sockets[0].getsockname()}')

    async with server:
        await server.serve_forever()

async def main():
    config = {
        "UPF": {
            "PORT": 50009,
            "BUFFER_SIZE": 1000
        },
        "GNB": {
            "PORT": 50015,
            "BUFFER_SIZE": 1000
        },
        "UE": {
            "PORT": 50011,
            "BUFFER_SIZE": 1000
        }
    }

    upf_rawdata_queue = RingBuffer(MAX_L1_UPF_DEPTH)
    upftask = asyncio.create_task(start_server("UPF", config, upf_rawdata_queue))

    gnb_rawdata_queue = RingBuffer(MAX_L1_GNB_DEPTH)
    gnbtask = asyncio.create_task(start_server("GNB", config, gnb_rawdata_queue))

    ue_rawdata_queue = RingBuffer(MAX_L1_UE_DEPTH)
    uetask = asyncio.create_task(start_server("UE", config, ue_rawdata_queue))

    processtask = asyncio.create_task(process_queues(upf_rawdata_queue, gnb_rawdata_queue, ue_rawdata_queue, config))

    try:
        await asyncio.gather(upftask, gnbtask, uetask, processtask)
    except KeyboardInterrupt:
        # Cancel the server tasks if the main thread is interrupted
        upftask.cancel()
        gnbtask.cancel()
        uetask.cancel()
        processtask.cancel()
        try:
            # Wait for the tasks to finish or raise the CancelledError
            await asyncio.gather(upftask, gnbtask, uetask, processtask, return_exceptions=True)
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle KeyboardInterrupt outside of asyncio.run
        pass