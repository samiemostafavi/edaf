import threading, traceback, time, os, json, asyncio, multiprocessing, queue
from collections import deque
from loguru import logger
from multiprocessing import Process, Queue

from edaf.core.common.timestamp import rdtsctotsOnline
from edaf.core.uplink.gnb import ProcessULGNB
from edaf.core.uplink.ue import ProcessULUE
from edaf.core.uplink.nlmt import process_ul_nlmt
from edaf.core.uplink.combine import CombineUL
from edaf.core.uplink.decompose import process_ul_journeys
from edaf.api.influx import InfluxClient, InfluxClientFULL

MAX_L1_UPF_DEPTH = 1000 # lines
MAX_L2_UPF_DEPTH = 100 # journeys
JOURNEYS_THRESHOLD_UPF = 20

MAX_L1_GNB_DEPTH = 1000 # lines
MAX_L2_GNB_DEPTH = 100 # journeys
RAW_LINES_THRESHOLD_GNB = 500
JOURNEYS_THRESHOLD_GNB = 20

MAX_L1_UE_DEPTH = 1000 # lines
MAX_L2_UE_DEPTH = 100 # journeys
RAW_LINES_THRESHOLD_UE = 500
JOURNEYS_THRESHOLD_UE = 20

LOGGING_PERIOD_SEC = 2

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

def pop_q_items(items_queue : multiprocessing.Queue, num_items):                
    items = []
    while len(items) != num_items:
        items.append(items_queue.get())
    return items

def combine_journeys(upf_journeys_queue, gnb_journeys_queue, ue_journeys_queue, config):

    # set standalone var
    if (gnb_journeys_queue is None) and (ue_journeys_queue is None):
        standalone = True
    else:
        standalone = False

    combineul = CombineUL(standalone=standalone)

    if config["influx_token"]:
        if standalone:
            influx_cli = InfluxClient(influx_db_address, config["influx_token"], bucket, org, point_name)
        else:
            influx_cli = InfluxClientFULL(influx_db_address, config["influx_token"], bucket, org, point_name, desired_fields)
        logger.info("[combine journeys] influxDB client initialized")
    else:
        influx_cli = None
        logger.warning("[combine journeys] influxDB client NONE")

    logger.info(f"[combine journeys] process starts.")
    
    while True:
        try:
            time.sleep(0.1)
            # Create e2e journeys
            if not standalone:
                upf_items = pop_q_items(upf_journeys_queue, JOURNEYS_THRESHOLD_UPF)
                gnb_items = pop_q_items(gnb_journeys_queue, JOURNEYS_THRESHOLD_GNB)
                ue_items = pop_q_items(ue_journeys_queue, JOURNEYS_THRESHOLD_UE)
                df = combineul.run(
                    upf_items,
                    gnb_items,
                    ue_items
                )
                df = process_ul_journeys(df)
            else:
                upf_items = pop_q_items(upf_journeys_queue, JOURNEYS_THRESHOLD_UPF)
                df = combineul.run(
                    upf_items,
                    None,
                    None,
                )
                df = process_ul_journeys(df,standalone=True)

            if df is not None:
                if len(df)>0:
                    # print(df)
                    logger.debug(f"[combine journeys] Pushing {len(df)} packet records to the database")
                    # push df to influxdb
                    if influx_cli:
                        influx_cli.push_dataframe(df)
                    else:
                        logger.warning(f"[combine journeys] Failed to push {len(df)} packet records to the database as influx cli is not setup.")
        except Exception as ex:
            logger.error(f"[combine journeys] {ex}")
            logger.warning(traceback.format_exc())


def queue_process(client_name, config, rawdata_queue, journeys_queue):

    stats_dropped_lines = 0
    stats_rcv_lines = 0
    stats_published_journeys = 0
    stats_dropped_journeys = 0
    start_time = time.time()

    if (rawdata_queue is None) or (journeys_queue is None):
        return

    if client_name == 'UE':
        ITEMS_PROCESS_LIMIT = RAW_LINES_THRESHOLD_UE
        rdts = rdtsctotsOnline("UE")
        proc = ProcessULUE()
    elif client_name == 'GNB':
        ITEMS_PROCESS_LIMIT = RAW_LINES_THRESHOLD_GNB
        rdts = rdtsctotsOnline("GNB")
        proc = ProcessULGNB()
    elif client_name == 'UPF':
        ITEMS_PROCESS_LIMIT = 1
        rdts = None
        proc = None

    logger.info(f"[{client_name} queue process] starts.")

    raw_inputs = []
    journeys = []
    while True:
        try:
            try:
                raw_inputs.append(rawdata_queue.get_nowait())
            except queue.Empty:
                pass

            if len(raw_inputs) >= ITEMS_PROCESS_LIMIT:
                # update stats
                stats_rcv_lines = stats_rcv_lines + len(raw_inputs)
                if client_name == 'UE' or client_name == 'GNB':
                    l1lines = rdts.return_rdtsctots(raw_inputs)
                    if client_name == 'UE':
                        l1lines.reverse()
                    if len(l1lines) > 0:
                        journeys = proc.run(l1lines)
                elif client_name == 'UPF':
                    journeys = process_ul_nlmt(raw_inputs)
                
                raw_inputs = []
                
                #update stats
                for journey in journeys:
                    try:
                        journeys_queue.put_nowait(journey)
                        stats_published_journeys = stats_published_journeys + len(journeys)
                    except queue.Full:
                        # update stats
                        stats_dropped_journeys = stats_dropped_journeys + 1

                journeys = []

            # print stats
            current_time = time.time()
            elapsed_time = current_time - start_time
            if int(elapsed_time) >= LOGGING_PERIOD_SEC:
                logger.info(f"[{client_name} queue process] received lines: {stats_rcv_lines}, dropped lines: {stats_dropped_lines}, published journeys: {stats_published_journeys}, dropped journeys: {stats_dropped_journeys}")
                start_time = current_time

        except Exception as ex:
            logger.error(f"[{client_name} queue process] {ex}")
            logger.warning(traceback.format_exc())
            # update stats, clean the queues
            stats_dropped_lines = stats_dropped_lines + len(raw_inputs)
            stats_dropped_journeys = stats_dropped_journeys + len(journeys)
            raw_inputs = []
            journeys = []


async def handle_client(reader, writer, client_name, config, rawdata_queue):
    init = True
    rem_str = ''
    stats_dropped_lines = 0
    stats_published_lines = 0
    start_time = time.time()

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
                    if line != 'test':
                        try:
                            rawdata_queue.put_nowait(line)
                            stats_published_lines = stats_published_lines + 1
                        except queue.Full:
                            stats_dropped_lines = stats_dropped_lines + 1
            else:
                if '\n' in message:
                    received_lines = message.splitlines()
                    received_lines[0] = rem_str + received_lines[0]
                    rem_str = ''
                    for line in received_lines[:-1]:
                        if line != 'test':
                            try:
                                rawdata_queue.put_nowait(line)
                                stats_published_lines = stats_published_lines + 1
                            except queue.Full:
                                stats_dropped_lines = stats_dropped_lines + 1
                    rem_str = received_lines[-1]
                else:
                    rem_str = rem_str + message

            # print stats
            current_time = time.time()
            elapsed_time = current_time - start_time
            if int(elapsed_time) >= LOGGING_PERIOD_SEC:
                logger.info(f"[{client_name} server] published lines: {stats_published_lines}, dropped lines: {stats_dropped_lines}")
                start_time = current_time
            
    except asyncio.CancelledError:
        pass
    finally:
        logger.warning(f"[{client_name} server] Closing the connection")
        writer.close()

async def async_net_server(client_name, config, rawdata_queue):
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

def net_server(client_name, config, rawdata_queue):
    asyncio.run(async_net_server(client_name, config, rawdata_queue))

def serve():

    # get version
    from .. import __version__
    logger.info(f"[main] Running EDAF server v{__version__}")

    # get standalone env variable
    standalone_var_str = os.environ.get("STANDALONE")
    if standalone_var_str is not None:
        standalone = standalone_var_str.lower() in ['true', '1', 'yes']
    else:
        standalone = False

    logger.info(f"[main] Standalone mode:{standalone}")

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
    upf_rawdata_queue = Queue(MAX_L1_UPF_DEPTH)
    upf_journeys_queue = Queue(MAX_L2_UPF_DEPTH)
    gnb_rawdata_queue = None
    gnb_journeys_queue = None
    ue_rawdata_queue = None
    ue_journeys_queue = None

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

        gnb_rawdata_queue = Queue(MAX_L1_GNB_DEPTH)
        gnb_journeys_queue = Queue(MAX_L2_GNB_DEPTH)
        ue_rawdata_queue = Queue(MAX_L1_UE_DEPTH)
        ue_journeys_queue = Queue(MAX_L2_UE_DEPTH)

    try:
        # UPF
        upf_server = Process(target=net_server, args=("UPF", config, upf_rawdata_queue),daemon=True)
        upf_qprocess = Process(target=queue_process, args=("UPF", config, upf_rawdata_queue, upf_journeys_queue),daemon=True)

        # GNB
        gnb_server = Process(target=net_server, args=("GNB", config, gnb_rawdata_queue),daemon=True)
        gnb_qprocess = Process(target=queue_process, args=("GNB", config, gnb_rawdata_queue, gnb_journeys_queue),daemon=True)

        # UE
        ue_server = Process(target=net_server, args=("UE", config, ue_rawdata_queue),daemon=True)
        ue_qprocess = Process(target=queue_process, args=("UE", config, ue_rawdata_queue, ue_journeys_queue),daemon=True)

        # COMBINE
        combine_process = Process(target=combine_journeys, args=(upf_journeys_queue, gnb_journeys_queue, ue_journeys_queue, config), daemon=True)
        
        # start
        upf_server.start()
        upf_qprocess.start()

        gnb_server.start()
        gnb_qprocess.start()

        ue_server.start()
        ue_qprocess.start()

        combine_process.start()

        # join
        upf_server.join()
        upf_qprocess.join()

        gnb_server.join()
        gnb_qprocess.join()

        ue_server.join()
        ue_qprocess.join()

        combine_process.join()

    except KeyboardInterrupt:
        logger.warning("Caught KeyboardInterrupt, terminating workers")

        # terminate
        upf_server.terminate()
        upf_qprocess.terminate()

        gnb_server.terminate()
        gnb_qprocess.terminate()

        ue_server.terminate()
        ue_qprocess.terminate()
        
        combine_process.terminate()
    else:
        logger.warning("Termination")

        # terminate
        upf_server.terminate()
        upf_qprocess.terminate()

        gnb_server.terminate()
        gnb_qprocess.terminate()

        ue_server.terminate()
        ue_qprocess.terminate()

        combine_process.terminate()


