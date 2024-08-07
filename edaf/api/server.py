import threading
from collections import deque
from loguru import logger
import asyncio
import os
import json

from edaf.core.common.timestamp import rdtsctotsOnline
from edaf.core.uplink.gnb import ProcessULGNB
from edaf.core.uplink.ue import ProcessULUE
from edaf.core.uplink.nlmt import process_ul_nlmt
from edaf.core.uplink.combine import CombineUL
from edaf.core.uplink.decompose import process_ul_journeys
from edaf.api.influx import InfluxClient, InfluxClientFULL

MAX_L1_UPF_DEPTH = 1000 # lines
MAX_L2_UPF_DEPTH = 100 # journeys

MAX_L1_GNB_DEPTH = 1000 # lines
MAX_L2_GNB_DEPTH = 100 # journeys

MAX_L1_UE_DEPTH = 1000 # lines
MAX_L2_UE_DEPTH = 100 # journeys

org = "expeca"
bucket = "latency"
influx_db_address = "http://0.0.0.0:8086"
auth_info_addr = "/EDAF/influx_auth.json"
point_name = "packet_records"

desired_fields = [
    "rlc.reassembled.num_segments"
    "core_delay",
    "core_delay_perc",
    "core_departure_time",
    "e2e_delay",
    "gtp.out.length",
    "gtp.out.sn",
    "ip.in.length",
    "link_delay",
    "link_delay_perc",
    "queuing_delay",
    "queuing_delay_perc",
    "radio_arrival_time_os",
    "radio_departure_time",
    "radio_departure_time_os",
    "ran_delay",
    "retransmission_delay",
    "retransmission_delay_perc",
    "rlc.queue.queue",
    "segmentation_delay",
    "segmentation_delay_perc",
    "seqno",
    "service_time",
    "service_time_os",
    "service_time_seg1",
    "service_time_seg1_os",
    "service_time_seg2",
    "service_time_seg2_os",
    "service_time_seg3",
    "service_time_seg3_os",
    "transmission_delay",
    "transmission_delay_perc"
]

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

    # set standalone var
    if (gnb_rawdata_queue is None) or (ue_rawdata_queue is None):
        standalone = True
    else:
        standalone = False

    upf_journeys_queue = RingBuffer(MAX_L2_UPF_DEPTH)
    combineul = CombineUL(standalone=standalone)
    gnb_journeys_queue, ue_journeys_queue, gnbrdts, gnbproc, uerdts, ueproc = None, None, None, None, None, None

    if config["influx_token"]:
        if standalone:
            influx_cli = InfluxClient(influx_db_address, config["influx_token"], bucket, org, point_name)
        else:
            influx_cli = InfluxClientFULL(influx_db_address, config["influx_token"], bucket, org, point_name, desired_fields)
        print("influxDB client initialized")
    else:
        influx_cli = None
        print("influxDB client None")

    if not standalone:
        gnb_journeys_queue = RingBuffer(MAX_L2_GNB_DEPTH)
        ue_journeys_queue = RingBuffer(MAX_L2_UE_DEPTH)
        gnbrdts = rdtsctotsOnline("GNB")
        gnbproc = ProcessULGNB()
        uerdts = rdtsctotsOnline("UE")
        ueproc = ProcessULUE()
        
    try:
        while True:
            await asyncio.sleep(0.1)
            upf_queue_length = upf_rawdata_queue.get_length()
            if not standalone:
                gnb_queue_length = gnb_rawdata_queue.get_length()
                ue_queue_length = ue_rawdata_queue.get_length()
            else:
                gnb_queue_length, ue_queue_length = 0,0
            
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
    
            # Create e2e journeys
            if not standalone:
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
                    if df is not None:
                        if len(df)>0:
                            # print(df)
                            logger.debug(f"Pushing {len(df)} packet records to the database")
                            # push df to influxdb
                            if influx_cli:
                                influx_cli.push_dataframe(df)
                            else:
                                logger.warning(f"Failed to push {len(df)} packet records to the database as influx cli is not setup.")
            else:
                upf_jrny_len = upf_journeys_queue.get_length()
                if upf_jrny_len > 20:
                    df = combineul.run(
                        upf_journeys_queue.pop_items(upf_jrny_len),
                        None,
                        None,
                    )
                    df = process_ul_journeys(df,standalone=True)
                    if df is not None:
                        if len(df)>0:
                            # print(df)
                            logger.debug(f"Pushing {len(df)} packet records to the database")
                            # push df to influxdb
                            if influx_cli:
                                influx_cli.push_dataframe(df)
                            else:
                                logger.warning(f"Failed to push {len(df)} packet records to the database as influx cli is not setup.")

                            

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
            message = data.decode(errors='ignore')
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
    if rawdata_queue is None:
        return

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, client_name, config, rawdata_queue), 
        host='0.0.0.0', 
        port=config[client_name]["PORT"]
    )
    
    logger.info(f'[{client_name} server] serving on {server.sockets[0].getsockname()}')

    async with server:
        await server.serve_forever()

async def run_serve():

    # get version
    from .. import __version__
    print(f"Running EDAF server v{__version__}")

    # get standalone env variable
    standalone_var_str = os.environ.get("STANDALONE")
    if standalone_var_str is not None:
        standalone = standalone_var_str.lower() in ['true', '1', 'yes']
    else:
        standalone = False

    print(f"Standalone mode:{standalone}")

    # read influxtoken
    try:
        # Read the JSON file
        with open(auth_info_addr) as f:
            data = json.load(f)

        # Extract token
        token = data[0]['token']
    except FileNotFoundError:
        # If the file doesn't exist, set token to None
        token = None

    config = {
        "influx_token" : token,
        "UPF": {
            "PORT": 50009,
            "BUFFER_SIZE": 1000
        }
    }
    upf_rawdata_queue = RingBuffer(MAX_L1_UPF_DEPTH)
    gnb_rawdata_queue = None
    ue_rawdata_queue = None

    if not standalone:
        config = {
            **config,
            "GNB": {
                "PORT": 50015,
                "BUFFER_SIZE": 1000
            },
            "UE": {
                "PORT": 50011,
                "BUFFER_SIZE": 1000
            }
        }

        gnb_rawdata_queue = RingBuffer(MAX_L1_GNB_DEPTH)
        ue_rawdata_queue = RingBuffer(MAX_L1_UE_DEPTH)

    upftask = asyncio.create_task(start_server("UPF", config, upf_rawdata_queue))
    gnbtask = asyncio.create_task(start_server("GNB", config, gnb_rawdata_queue))
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

def serve():

    try:
        asyncio.run(run_serve())
    except KeyboardInterrupt:
        # Handle KeyboardInterrupt outside of asyncio.run
        pass
