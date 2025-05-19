import threading, traceback, time, os, json, asyncio, multiprocessing, queue
from collections import deque
from loguru import logger
from multiprocessing import Process, Queue
import pandas as pd

from edaf.core.common.timestamp import rdtsctotsOnline
from edaf.core.uplink.gnb.gnb import ProcessULGNB
from edaf.core.uplink.ue.ue import ProcessULUE
from edaf.core.uplink.nlmt import process_ul_nlmt
from edaf.core.uplink.analyze_packet import ULPacketAnalyzer
from edaf.api.influx import InfluxClient

MAX_L1_UPF_DEPTH = 5000 # lines
MAX_L2_UPF_DEPTH = 500 # journeys
JOURNEYS_THRESHOLD_UPF = 20

MAX_L1_GNB_DEPTH = 5000 # lines
MAX_L2_GNB_DEPTH = 500 # journeys
RAW_LINES_THRESHOLD_GNB = 500
JOURNEYS_THRESHOLD_GNB = 20

MAX_L1_UE_DEPTH = 5000 # lines
MAX_L2_UE_DEPTH = 500 # journeys
RAW_LINES_THRESHOLD_UE = 500
JOURNEYS_THRESHOLD_UE = 20

LOGGING_PERIOD_SEC = 2

PACKET_ANALYZE_SLEEP_S = 1 # while loop sleep duration in seconds
MIN_NUM_PACKETS_TO_ANALYZE = 200

org = "expeca"
bucket = "edaf_raw"
influx_db_address = "http://0.0.0.0:8086"
auth_info_addr = "/EDAF/influx_auth.json"


def pop_q_items(items_queue : multiprocessing.Queue):                
    items = []
    while not items_queue.empty():
        items.append(items_queue.get())
    return items


def analyze_and_publish(upf_journeys_queue, gnb_ip_packets_queue, gnb_rlc_segments_queue, gnb_mac_attempts_queue, gnb_mcs_reports_queue, ue_ip_packets_queue, ue_rlc_segments_queue, ue_mac_attempts_queue, config):
    # stats counters initialize
    stats_rcv_upf, stats_rcv_gnb_ip, stats_rcv_gnb_rlc, stats_rcv_gnb_mac, stats_rcv_gnb_mcs, stats_rcv_ue_ip, stats_rcv_ue_rlc, stats_rcv_ue_mac = 0, 0, 0, 0, 0, 0, 0, 0
    stats_published_upf_items, stats_published_gnb_ip_items, stats_published_gnb_rlc_items, stats_gnb_mac_items, stats_gnb_mcs_items, stats_ue_ip_items, stats_ue_rlc_items, stats_ue_mac_items = 0, 0, 0, 0, 0, 0, 0, 0

    start_time = time.time()

    if config["influx_token"]:
        influx_cli = InfluxClient(influx_db_address, config["influx_token"], bucket, org)
        logger.success("[combine journeys] influxDB client initialized")
    else:
        influx_cli = None
        logger.error("[combine journeys] influxDB client NONE")

    logger.success(f"[combine journeys] process starts.")
    
    while True:
        
        time.sleep(PACKET_ANALYZE_SLEEP_S)

        # print stats
        current_time = time.time()
        elapsed_time = current_time - start_time
        if int(elapsed_time) >= LOGGING_PERIOD_SEC:
            logger.info(f"[received data to publish] received entries: UPF {stats_rcv_upf}, GNB.ip {stats_rcv_gnb_ip}, GNB.rlc {stats_rcv_gnb_rlc}, GNB.mac {stats_rcv_gnb_mac}, GNB.mcs {stats_rcv_gnb_mcs}, UE.ip {stats_rcv_ue_ip}, UE.rlc {stats_rcv_ue_rlc}, UE.mac {stats_rcv_ue_mac}")
            logger.info(f"[actually published data to influxdb] \t   UPF {stats_published_upf_items}, GNB.ip {stats_published_gnb_ip_items}, GNB.rlc {stats_published_gnb_rlc_items}, GNB.mac {stats_gnb_mac_items}, GNB.mcs {stats_gnb_mcs_items}, UE.ip {stats_ue_ip_items}, UE.rlc {stats_ue_rlc_items}, UE.mac {stats_ue_mac_items}")

            start_time = current_time
    
        try:

            upf_items = pop_q_items(upf_journeys_queue)
            stats_rcv_upf += len(upf_items)
            upf_items_df = pd.DataFrame(upf_items) 
            
            gnb_ip_items = pop_q_items(gnb_ip_packets_queue)
            stats_rcv_gnb_ip += len(gnb_ip_items)
            gnb_ip_items_df = pd.DataFrame(gnb_ip_items)

            gnb_rlc_items = pop_q_items(gnb_rlc_segments_queue)
            stats_rcv_gnb_rlc += len(gnb_rlc_items)
            gnb_rlc_items_df = pd.DataFrame(gnb_rlc_items)

            gnb_mac_items = pop_q_items(gnb_mac_attempts_queue)
            stats_rcv_gnb_mac += len(gnb_mac_items)
            gnb_mac_items_df = pd.DataFrame(gnb_mac_items)

            gnb_mcs_items = pop_q_items(gnb_mcs_reports_queue)
            stats_rcv_gnb_mcs += len(gnb_mcs_items)
            gnb_mcs_items_df = pd.DataFrame(gnb_mcs_items)

            ue_ip_items = pop_q_items(ue_ip_packets_queue)
            stats_rcv_ue_ip += len(ue_ip_items)
            ue_ip_items_df = pd.DataFrame(ue_ip_items)

            ue_rlc_items = pop_q_items(ue_rlc_segments_queue)
            stats_rcv_ue_rlc += len(ue_rlc_items)
            ue_rlc_items_df = pd.DataFrame(ue_rlc_items)

            ue_mac_items = pop_q_items(ue_mac_attempts_queue)
            stats_rcv_ue_mac += len(ue_mac_items)
            ue_mac_items_df = pd.DataFrame(ue_mac_items)  

            # Create gnb databases relationship
            # For each 'gtp.out.sn' in gnb_ip_packets_df, find corresponding 'sdu_id' entries in gnb_rlc_segments_df
            #gnb_iprlc_rel_df = pd.merge(gnb_ip_items_df[['gtp.out.sn']],
            #                        gnb_rlc_items_df[['rlc.reassembled.sn', 'sdu_id']],
            #                        left_on='gtp.out.sn', right_on='rlc.reassembled.sn')
            #gnb_iprlc_rel_df = gnb_iprlc_rel_df.drop(columns=['rlc.reassembled.sn'])

            # For each pair of ['rlc.queue.R2buf', 'rlc.queue.sn'] in ue_ip_packets_df,
            # find corresponding entries in ue_rlc_segments_df with the same values for ['rlc.txpdu.R2buf', 'rlc.txpdu.sn']
            #ue_iprlc_rel_df = pd.merge(ue_ip_items_df[['rlc.queue.R2buf', 'rlc.queue.sn',  'ip_id']],
            #                        ue_rlc_items_df[['rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.srn','rlc.txpdu.timestamp', 'rlc.txpdu.length', 'txpdu_id']],  # Additional columns from ue_rlc_segments_df
            #                        left_on=['rlc.queue.R2buf', 'rlc.queue.sn'],
            #                        right_on=['rlc.txpdu.R2buf', 'rlc.txpdu.sn'])
            #ue_iprlc_rel_df = ue_iprlc_rel_df.drop(columns=['rlc.queue.R2buf', 'rlc.queue.sn' , 'rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.timestamp', 'rlc.txpdu.length'])

            if influx_cli:

                publish_list = []

                if len(upf_items_df) > 0:
                    # Convert to numeric (force errors to NaN if needed)
                    upf_items_df["st"] = pd.to_numeric(upf_items_df["st"], errors="coerce")
                    upf_items_df["rt"] = pd.to_numeric(upf_items_df["rt"], errors="coerce")

                    # Drop rows with NaN timestamps if necessary
                    upf_items_df.dropna(subset=["st", "rt"], inplace=True)

                    # Now safe to do floor division
                    upf_items_df["st_sec"] = upf_items_df["st"] // 1_000_000_000
                    upf_items_df["rt_sec"] = upf_items_df["rt"] // 1_000_000_000

                    # Push to InfluxDB
                    publish_list.append(
                        { "df" : upf_items_df, "point_name": "upf", "time_key": "st_sec" }
                    )
                    stats_published_upf_items += len(upf_items_df)

                if len(gnb_ip_items_df)>0:
                    publish_list.append(
                        { "df" : gnb_ip_items_df, "point_name": "gnb_ip", "time_key": "gtp.out.timestamp" }
                    )
                    stats_published_gnb_ip_items += len(gnb_ip_items_df)

                if len(gnb_rlc_items_df)>0:
                    publish_list.append(
                        { "df" : gnb_rlc_items_df, "point_name": "gnb_rlc", "time_key": "rlc.reassembled.timestamp" }
                    )
                    stats_published_gnb_rlc_items += len(gnb_rlc_items_df)

                if len(gnb_mac_items_df)>0:
                    publish_list.append(
                        { "df" : gnb_mac_items_df, "point_name": "gnb_mac", "time_key": "phy.detectstart.timestamp" }
                    )
                    stats_gnb_mac_items += len(gnb_mac_items_df)

                if len(gnb_mcs_items_df)>0:
                    publish_list.append(
                        { "df" : gnb_mcs_items_df, "point_name": "gnb_mcs", "time_key": "timestamp" }
                    )
                    stats_gnb_mcs_items += len(gnb_mcs_items_df)

                if len(ue_ip_items_df)>0:
                    publish_list.append(
                        { "df" : ue_ip_items_df, "point_name": "ue_ip", "time_key": "ip.in.timestamp" }
                    )
                    stats_ue_ip_items += len(ue_ip_items_df)

                if len(ue_rlc_items_df)>0:
                    publish_list.append(
                        { "df" : ue_rlc_items_df, "point_name": "ue_rlc", "time_key": "rlc.txpdu.timestamp" }
                    )
                    stats_ue_rlc_items += len(ue_rlc_items_df)

                if len(ue_mac_items_df)>0:
                    publish_list.append(
                        { "df" : ue_mac_items_df, "point_name": "ue_mac", "time_key": "mac.harq.timestamp" }
                    )
                    stats_ue_mac_items += len(ue_mac_items_df)

                if publish_list:
                    influx_cli.push_dataframe_list(publish_list)
                
            else:
                logger.warning(f"[combine journeys] Failed to push {len(upf_items_df)} upf records to the database as influx cli is not setup.")

                #packet_analyzer = ULPacketAnalyzer(upf_items_df, gnb_ip_items_df, gnb_rlc_items_df, gnb_iprlc_rel_df, gnb_mac_items_df, gnb_mcs_items_df, ue_ip_items_df, ue_rlc_items_df, ue_mac_items_df, ue_iprlc_rel_df)
                #uids_arr = list(range(int(packet_analyzer.first_ueipid), int(packet_analyzer.last_ueipid)))
                #analyzed_packets_list = packet_analyzer.figure_packettx_from_ueipids(uids_arr)
                #analyzed_packets_df = pd.DataFrame(analyzed_packets_list)
            

            #if analyzed_packets_df is not None:
            #    if len(analyzed_packets_df)>0:
                    # print(df)
            #        logger.debug(f"[combine journeys] Pushing {len(analyzed_packets_df)} packet records to the database")
                    # push df to influxdb
            #        if influx_cli:
            #            influx_cli.push_dataframe(analyzed_packets_df)
            #            stats_published_packets += len(analyzed_packets_df)
            #        else:
            #            logger.warning(f"[combine journeys] Failed to push {len(analyzed_packets_df)} packet records to the database as influx cli is not setup.")
        except Exception as ex:
            logger.error(f"[analyze packets] {ex}")
            logger.warning(traceback.format_exc())


def queue_process(
            client_name, config, 
            rawdata_queue, 
            ue_ip_packets_queue, ue_rlc_segments_queue, ue_mac_attempts_queue,
            gnb_ip_packets_queue, gnb_rlc_segments_queue, gnb_mac_attempts_queue, gnb_mcs_reports_queue,
            upf_journeys_queue
        ):

    stats_dropped_lines = 0
    stats_rcv_lines = 0
    
    stats_published_upf_journeys = 0
    stats_dropped_upf_journeys = 0

    stats_published_ue_ip_packets = 0
    stats_dropped_ue_ip_packets = 0
    stats_published_ue_rlc_segments = 0
    stats_dropped_ue_rlc_segments = 0
    stats_published_ue_mac_attempts = 0
    stats_dropped_ue_mac_attempts = 0

    stats_published_gnb_ip_packets = 0
    stats_dropped_gnb_ip_packets = 0
    stats_published_gnb_rlc_segments = 0
    stats_dropped_gnb_rlc_segments = 0
    stats_published_gnb_mac_attempts = 0
    stats_dropped_gnb_mac_attempts = 0
    stats_published_gnb_mcs_reports = 0
    stats_dropped_gnb_mcs_reports = 0

    start_time = time.time()

    if client_name == 'UE':
        if (rawdata_queue is None) or (ue_ip_packets_queue is None):
            return
        ITEMS_PROCESS_LIMIT = RAW_LINES_THRESHOLD_UE
        rdts = rdtsctotsOnline("UE")
        proc = ProcessULUE(
            enable_ip_packets = True,
            enable_rlc_segments = True,
            enable_mac_attempts = True,
            enable_uldcis_reports = False,
            enable_sched_reports = False
        )
    elif client_name == 'GNB':
        if (rawdata_queue is None) or (gnb_ip_packets_queue is None):
            return
        ITEMS_PROCESS_LIMIT = RAW_LINES_THRESHOLD_GNB
        rdts = rdtsctotsOnline("GNB")
        proc = ProcessULGNB(
            enable_ip_packets = True,
            enable_rlc_segments = True,
            enable_sched_reports = False,
            enable_sched_maps = False,
            enable_rlc_reports = False,
            enable_mac_attempts = True,
            enable_mcs_reports = True
        )
    elif client_name == 'UPF':
        if (rawdata_queue is None) or (upf_journeys_queue is None):
            return
        ITEMS_PROCESS_LIMIT = 1
        rdts = None
        proc = None

    logger.success(f"[{client_name} queue process] starts.")

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
                if client_name == 'UE':

                    l1lines = rdts.return_rdtsctots(raw_inputs)
                    raw_inputs = []  
                    l1lines.reverse()
                    if len(l1lines) > 0:
                        ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df, _, _, _, _, _ = proc.run(l1lines)
                        for index, row in ue_ip_packets_df.iterrows():
                            try:
                                ue_ip_packets_queue.put_nowait(row)
                                stats_published_ue_ip_packets += 1
                            except queue.Full:
                                # update stats
                                stats_dropped_ue_ip_packets += 1

                        for index, row in ue_rlc_segments_df.iterrows():
                            try:
                                ue_rlc_segments_queue.put_nowait(row)
                                stats_published_ue_rlc_segments += 1
                            except queue.Full:
                                # update stats
                                stats_dropped_ue_rlc_segments += 1

                        for index, row in ue_mac_attempts_df.iterrows():
                            try:
                                ue_mac_attempts_queue.put_nowait(row)
                                stats_published_ue_mac_attempts += 1
                            except queue.Full:
                                # update stats
                                stats_dropped_ue_mac_attempts += 1

                        ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df = [], [], []
                        
                elif client_name == 'GNB':

                    l1lines = rdts.return_rdtsctots(raw_inputs)
                    raw_inputs = []  
                    if len(l1lines) > 0:
                        gnb_ip_packets_df, gnb_rlc_segments_df, _, _, _, gnb_mac_attempts_df, gnb_mcs_reports_df = proc.run(l1lines)

                        for index, row in gnb_ip_packets_df.iterrows():
                            try:
                                gnb_ip_packets_queue.put_nowait(row)
                                stats_published_gnb_ip_packets += 1
                            except queue.Full:
                                # update stats
                                stats_dropped_gnb_ip_packets += 1

                        for index, row in gnb_rlc_segments_df.iterrows():
                            try:
                                gnb_rlc_segments_queue.put_nowait(row)
                                stats_published_gnb_rlc_segments += 1
                            except queue.Full:
                                # update stats
                                stats_dropped_gnb_rlc_segments += 1

                        for index, row in gnb_mac_attempts_df.iterrows():
                            try:
                                gnb_mac_attempts_queue.put_nowait(row)
                                stats_published_gnb_mac_attempts += 1
                            except queue.Full:
                                # update stats
                                stats_dropped_gnb_mac_attempts += 1

                        for index, row in gnb_mcs_reports_df.iterrows():
                            try:
                                gnb_mcs_reports_queue.put_nowait(row)
                                stats_published_gnb_mcs_reports += 1
                            except queue.Full:
                                # update stats
                                stats_dropped_gnb_mcs_reports += 1
                        
                        gnb_ip_packets_df, gnb_rlc_segments_df, gnb_mac_attempts_df, gnb_mcs_reports_df = [], [], [], []

                elif client_name == 'UPF':
                    upf_journeys = process_ul_nlmt(raw_inputs)
                    raw_inputs = []            
                    #update stats
                    for upf_journey in upf_journeys:
                        try:
                            upf_journeys_queue.put_nowait(upf_journey)
                            stats_published_upf_journeys = stats_published_upf_journeys + 1
                        except queue.Full:
                            # update stats
                            stats_dropped_upf_journeys = stats_dropped_upf_journeys + 1
                    upf_journeys = []

            # print stats
            current_time = time.time()
            elapsed_time = current_time - start_time
            if int(elapsed_time) >= LOGGING_PERIOD_SEC:
                if client_name == 'UE':
                    logger.info(f"[{client_name} queue process] received lines: {stats_rcv_lines}, dropped lines: {stats_dropped_lines}, published ue_ip_packets: {stats_published_ue_ip_packets}, dropped ue_ip_packets: {stats_dropped_ue_ip_packets}")
                    start_time = current_time
                elif client_name == 'GNB':
                    logger.info(f"[{client_name} queue process] received lines: {stats_rcv_lines}, dropped lines: {stats_dropped_lines}, published gnb_ip_packets: {stats_published_gnb_ip_packets}, dropped gnb_ip_packets: {stats_dropped_gnb_ip_packets}")
                    start_time = current_time
                elif client_name == 'UPF':
                    logger.info(f"[{client_name} queue process] received lines: {stats_rcv_lines}, dropped lines: {stats_dropped_lines}, published UPF journeys: {stats_published_upf_journeys}, dropped UPF journeys: {stats_dropped_upf_journeys}")
                    start_time = current_time

        except Exception as ex:
            logger.error(f"[{client_name} queue process] {ex}")
            logger.warning(traceback.format_exc())
            # update stats, clean the queues
            stats_dropped_lines = stats_dropped_lines + len(raw_inputs)
            raw_inputs = []
            ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df = [], [], []
            gnb_ip_packets_df, gnb_rlc_segments_df, gnb_mac_attempts_df, gnb_mcs_reports_df = [], [], [], []
            upf_journeys = []


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
                logger.success(f"[{client_name} server] connection from {addr}.")
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
    
    logger.success(f'[{client_name} server] serving on {server.sockets[0].getsockname()}')

    async with server:
        await server.serve_forever()

def net_server(client_name, config, rawdata_queue):
    asyncio.run(async_net_server(client_name, config, rawdata_queue))

def serve():

    # get version
    from .. import __version__
    logger.success(f"[main] Running EDAF server v{__version__}")

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

    upf_rawdata_queue = Queue(MAX_L1_UPF_DEPTH)
    upf_journeys_queue = Queue(MAX_L2_UPF_DEPTH)
    gnb_rawdata_queue = None
    ue_rawdata_queue = None
    
    config = {
        "influx_token" : token,
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

    gnb_rawdata_queue = Queue(MAX_L1_GNB_DEPTH)
    ue_rawdata_queue = Queue(MAX_L1_UE_DEPTH)
    gnb_ip_packets_queue, gnb_rlc_segments_queue, gnb_mac_attempts_queue, gnb_mcs_reports_queue = Queue(MAX_L2_GNB_DEPTH), Queue(MAX_L2_GNB_DEPTH), Queue(MAX_L2_GNB_DEPTH), Queue(MAX_L2_GNB_DEPTH)
    ue_ip_packets_queue, ue_rlc_segments_queue, ue_mac_attempts_queue = Queue(MAX_L2_UE_DEPTH), Queue(MAX_L2_UE_DEPTH), Queue(MAX_L2_UE_DEPTH)

    try:
        # UPF
        upf_server = Process(target=net_server, args=("UPF", config, upf_rawdata_queue),daemon=True)
        upf_qprocess = Process(target=queue_process, args=("UPF", config, upf_rawdata_queue, None, None, None, None, None, None, None, upf_journeys_queue),daemon=True)

        # GNB
        gnb_server = Process(target=net_server, args=("GNB", config, gnb_rawdata_queue),daemon=True)
        gnb_qprocess = Process(target=queue_process, args=("GNB", config, gnb_rawdata_queue, None, None, None, gnb_ip_packets_queue, gnb_rlc_segments_queue, gnb_mac_attempts_queue, gnb_mcs_reports_queue, None),daemon=True)

        # UE
        ue_server = Process(target=net_server, args=("UE", config, ue_rawdata_queue),daemon=True)
        ue_qprocess = Process(target=queue_process, args=("UE", config, ue_rawdata_queue, ue_ip_packets_queue, ue_rlc_segments_queue, ue_mac_attempts_queue, None, None, None, None, None),daemon=True)

        # COMBINE
        combine_process = Process(target=analyze_and_publish, args=(upf_journeys_queue, gnb_ip_packets_queue, gnb_rlc_segments_queue, gnb_mac_attempts_queue, gnb_mcs_reports_queue, ue_ip_packets_queue, ue_rlc_segments_queue, ue_mac_attempts_queue, config), daemon=True)
        
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