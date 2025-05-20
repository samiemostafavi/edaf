import threading, traceback, time, os, json, asyncio, multiprocessing, queue, copy
from collections import deque
from loguru import logger
from multiprocessing import Process, Queue
from multiprocessing import Manager

import pandas as pd

from edaf.core.common.timestamp import rdtsctotsOnline
from edaf.core.uplink.gnb.gnb import ProcessULGNB
from edaf.core.uplink.ue.ue import ProcessULUE
from edaf.core.uplink.nlmt import process_ul_nlmt
from edaf.core.uplink.analyze_packet import ULPacketAnalyzer
from edaf.api.influx import InfluxClient

MAX_L1_UPF_DEPTH = 40 # lines in one page
QPROC_SLEEP_UPF_S = 0.5 # how often read one page and process it

MAX_L1_GNB_DEPTH = 5000 # lines in one page
QPROC_SLEEP_GNB_S = 0.5 # how often read one page and process it

MAX_L1_UE_DEPTH = 2500 # lines in one page
QPROC_SLEEP_UE_S = 0.5 # how often read one page and process it

LOGGING_PERIOD_SEC = 2

PACKETS_DECOMPOSE_WINDOW_MS = 2000 # history window duration in seconds
PACKETS_DECOMPOSE_SLEEP_S = 0.1 # while loop sleep duration in seconds

org = "expeca"
raw_bucket = "edaf_raw"
main_bucket = "edaf_main"
influx_db_address = "http://0.0.0.0:8086"
auth_info_addr = "/EDAF/influx_auth.json"


class SharedRingBuffer:
    def __init__(self, size, manager):
        self.size = size
        self.buffer = manager.list()
        self.lock = manager.Lock()

    def append(self, item):
        with self.lock:
            if len(self.buffer) >= self.size:
                self.buffer.pop(0)  # manually remove oldest
            self.buffer.append(item)

    def peek_head(self):
        with self.lock:
            if len(self.buffer) == 0:
                return None
            return copy.deepcopy(self.buffer[0])

    def get_items(self):
        with self.lock:
            return copy.deepcopy(list(self.buffer))

    def pop_items(self, n):
        with self.lock:
            popped = []
            for _ in range(min(n, len(self.buffer))):
                popped.append(self.buffer.pop(0))
            return popped

    def get_length(self):
        with self.lock:
            return len(self.buffer)


def pop_q_items(items_queue : multiprocessing.Queue):                
    items = []
    while not items_queue.empty():
        items.append(items_queue.get())
    return items


def decompose_packet_delays(flat_packet, complete_packet):
    delays = {
        "e2e_delay": None,
        "queuing_delay": None,
        "link_delay": None,
        "segmentation_delay": None,
        "transmission_delay": None,
        "retransmission_delay": None
    }

    # End-to-end delay
    try:
        delays["e2e_delay"] = (flat_packet["ip.out_t"] - flat_packet["ip.in_t"])*1000
    except:
        return delays  # Cannot calculate anything without E2E

    # get RLC attempts
    rlc_attempts = complete_packet.get("rlc.attempts", [])
    if not rlc_attempts:
        return delays

    rlc0 = rlc_attempts[0]
    rlcN = rlc_attempts[-1]

    # Segmentation delay
    try:
        delays["segmentation_delay"] = (rlcN['mac.out_t'] - rlc0['mac.out_t'])*1000
    except:
        pass

    rlc0_mac_attempts = rlc0.get("mac.attempts",[])
    if not rlc0_mac_attempts:
        return delays
    
    # Queuing delay
    try:
        delays["queuing_delay"] = (rlc0_mac_attempts[0]['phy.in_t'] - flat_packet["ip.in_t"])*1000
    except:
        pass

    # Link delay
    try:
        delays["link_delay"] =  (flat_packet['ip.out_t'] - rlc0_mac_attempts[0]['phy.in_t'])*1000
    except:
        pass

    # transmission delay
    try:
        delays["transmission_delay"] = (rlc0_mac_attempts[0]["phy.out_t"] - rlc0_mac_attempts[0]['phy.in_t'])*1000
    except:
        pass

    rlc_max = max(rlc_attempts, key=lambda r: len(r.get("mac.attempts", [])))
    rlc_max_mac_attempts = rlc_max.get("mac.attempts", [])
    if not rlc_max_mac_attempts:
        return delays

    # MAC retransmission delay
    try:
        if flat_packet['mac.num_total_retx'] > 0:
            delays["retransmission_delay"] = (rlc_max_mac_attempts[-1]["phy.out_t"] - rlc_max_mac_attempts[0]["phy.decode_t"])*1000
        else:
            delays["retransmission_delay"] = 0.0
    except:
        pass

    return delays


def flatten_decomposed_packets(decomposed_packets_list: list):
    flat_packets = []

    for dec_packet in decomposed_packets_list:
        rlc_attempts = dec_packet.get('rlc.attempts')

        if not rlc_attempts:
            # If rlc_attempts is None or empty, set all dependent metrics to None
            res_flat_packet = {
                'sn': dec_packet.get('sn'),
                'ip.len': dec_packet.get('len'),
                'ip.in_t': dec_packet.get('ip.in_t'),
                'ip.out_t': dec_packet.get('ip.out_t'),
                # #FIXME: gtp.out.timestamp (ip.out_t) sometimes gives a very large offset: 450ms later than rlc out which is wrong
                # for now, we should use rlc.out_t
                'rlc.in_t': dec_packet.get('rlc.in_t'),
                'rlc.out_t': dec_packet.get('rlc.out_t'),
                'backlog': dec_packet.get('backlog'),
                'rlc.total_num_attempts': None,
                'rlc.num_repeated_attempts': None,
                'mac.num_total_retx': None,
                'mac.num_max_retx': None,
                'mac.num_total_rbs': None,
                'mac.num_max_rbs': None,
                'mac.num_total_syms': None,
                'mac.num_max_syms': None
            }
            continue

        # Filter out any None entries in rlc_attempts
        rlc_attempts = [a for a in rlc_attempts if a]

        rlc_total_num_attempts = len(rlc_attempts)
        rlc_num_repeated_attempts = sum(int(a.get('repeated', 0)) for a in rlc_attempts)

        mac_retx_counts = []
        mac_rbs_values = []
        mac_syms_values = []

        for rlc_attempt in rlc_attempts:
            mac_attempts = rlc_attempt.get('mac.attempts')

            if not mac_attempts:
                mac_retx_counts.append(None)
                mac_rbs_values.append(None)
                mac_syms_values.append(None)
                continue

            mac_attempts = [m for m in mac_attempts if m]  # filter out None

            if not mac_attempts:
                mac_retx_counts.append(None)
                mac_rbs_values.append(None)
                mac_syms_values.append(None)
                continue

            mac_retx_counts.append(len(mac_attempts) - 1)
            mac_rbs_values.append(int(mac_attempts[0].get('rbs', 0)))
            mac_syms_values.append(int(mac_attempts[0].get('symbols', 0)))

        def safe_sum(values):
            if any(v is None for v in values):
                return None
            return sum(values)

        def safe_max(values):
            if any(v is None for v in values):
                return None
            return max(values)

        res_flat_packet = {
            'sn': dec_packet.get('sn'),
            'ip.len': dec_packet.get('len'),
            'ip.in_t': dec_packet.get('ip.in_t'),
            'ip.out_t': dec_packet.get('ip.out_t'),
            # #FIXME: gtp.out.timestamp (ip.out_t) is sometimes gives a very large offset: 450ms later than rlc out which is wrong
            # for now, we should use rlc.out_t
            'rlc.in_t': dec_packet.get('rlc.in_t'),
            'rlc.out_t': dec_packet.get('rlc.out_t'),
            'backlog': dec_packet.get('backlog'),
            'rlc.total_num_attempts': rlc_total_num_attempts,
            'rlc.num_repeated_attempts': rlc_num_repeated_attempts,
            'mac.num_total_retx': safe_sum(mac_retx_counts),
            'mac.num_max_retx': safe_max(mac_retx_counts),
            'mac.num_total_rbs': safe_sum(mac_rbs_values),
            'mac.num_max_rbs': safe_max(mac_rbs_values),
            'mac.num_total_syms': safe_sum(mac_syms_values),
            'mac.num_max_syms': safe_max(mac_syms_values)
        }

        delays = decompose_packet_delays(res_flat_packet, dec_packet)
        res_flat_packet = {
            **res_flat_packet,
            **delays
        }
        flat_packets.append(res_flat_packet)

    return flat_packets


def packets_decompose(config):
    # stats counters initialize
    stats_rcv_upf, stats_rcv_gnb_ip, stats_rcv_gnb_rlc, stats_rcv_gnb_mac, stats_rcv_gnb_mcs, stats_rcv_ue_ip, stats_rcv_ue_rlc, stats_rcv_ue_mac, stats_published_packets, stats_decomposed_packets = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    
    start_time = time.time()

    if config["influx_token"]:
        influx_cli = InfluxClient(influx_db_address, config["influx_token"], org)
        influx_cli.create_bucket(main_bucket)
        logger.success("[packets_decompose] influxDB client initialized")
    else:
        influx_cli = None
        logger.error("[packets_decompose] influxDB client NONE")

    logger.success(f"[packets_decompose] process starts.")


    while True:
        
        time.sleep(PACKETS_DECOMPOSE_SLEEP_S)

        # print stats
        current_time = time.time()
        elapsed_time = current_time - start_time
        if int(elapsed_time) >= LOGGING_PERIOD_SEC:
            logger.info(f"[packets_decompose]  downloaded items: UPF {stats_rcv_upf}, GNB.ip {stats_rcv_gnb_ip}, GNB.rlc {stats_rcv_gnb_rlc}, GNB.mac {stats_rcv_gnb_mac}, GNB.mcs {stats_rcv_gnb_mcs}, UE.ip {stats_rcv_ue_ip}, UE.rlc {stats_rcv_ue_rlc}, UE.mac {stats_rcv_ue_mac}")
            logger.info(f"[packets_decompose]  decomposed packets: {stats_decomposed_packets}, published packets: {stats_published_packets}")
            start_time = current_time
        try: 
            # run the decomposition and analysis
            upf_items_df, gnb_ip_items_df, gnb_rlc_items_df, gnb_mac_items_df, gnb_mcs_items_df, ue_ip_items_df, ue_rlc_items_df, ue_mac_items_df = None, None, None, None, None, None, None, None

            # fetch all the data from the last few seconds
            recent_data = influx_cli.fetch_recent_data(raw_bucket, PACKETS_DECOMPOSE_WINDOW_MS)
            for item in recent_data:
                # item is a dict with "df", "point_name", and "time_key"
                # let's recreate the dataframes:
                if item["point_name"] == "upf":
                    if len(item["df"]) > 0:
                        upf_items_df = item["df"]
                        stats_rcv_upf += len(item["df"])
                if item["point_name"] == "gnb_ip":
                    if len(item["df"]) > 0:
                        gnb_ip_items_df = item["df"]
                        stats_rcv_gnb_ip += len(item["df"])
                if item["point_name"] == "gnb_rlc":
                    if len(item["df"]) > 0:
                        gnb_rlc_items_df = item["df"]
                        stats_rcv_gnb_rlc += len(item["df"])
                if item["point_name"] == "gnb_mac":
                    if len(item["df"]) > 0:
                        gnb_mac_items_df = item["df"]
                        stats_rcv_gnb_mac += len(item["df"])
                if item["point_name"] == "gnb_mcs":
                    if len(item["df"]) > 0:
                        gnb_mcs_items_df = item["df"]
                        stats_rcv_gnb_mcs += len(item["df"])
                if item["point_name"] == "ue_ip":
                    if len(item["df"]) > 0:
                        ue_ip_items_df = item["df"]
                        stats_rcv_ue_ip += len(item["df"])
                if item["point_name"] == "ue_rlc":
                    if len(item["df"]) > 0:
                        ue_rlc_items_df = item["df"]
                        stats_rcv_ue_rlc += len(item["df"])
                if item["point_name"] == "ue_mac":
                    if len(item["df"]) > 0:
                        ue_mac_items_df = item["df"]
                        stats_rcv_ue_mac += len(item["df"])
                
            if all([
                    upf_items_df is not None,
                    gnb_ip_items_df is not None,
                    gnb_rlc_items_df is not None,
                    gnb_mac_items_df is not None,
                    gnb_mcs_items_df is not None,
                    ue_ip_items_df is not None,
                    ue_rlc_items_df is not None,
                    ue_mac_items_df is not None,
                ]):

                # Create gnb databases relationship
                # For each 'gtp.out.sn' in gnb_ip_packets_df, find corresponding 'sdu_id' entries in gnb_rlc_segments_df
                gnb_ip_items_df['gtp.out.sn'] = gnb_ip_items_df['gtp.out.sn'].astype(int)
                gnb_rlc_items_df['rlc.reassembled.sn'] = gnb_rlc_items_df['rlc.reassembled.sn'].astype(int)
                gnb_rlc_items_df['sdu_id'] = gnb_rlc_items_df['sdu_id'].astype(int)

                gnb_iprlc_rel_df = pd.merge(gnb_ip_items_df[['gtp.out.sn']],
                                        gnb_rlc_items_df[['rlc.reassembled.sn', 'sdu_id']],
                                        left_on='gtp.out.sn', right_on='rlc.reassembled.sn')
                gnb_iprlc_rel_df = gnb_iprlc_rel_df.drop(columns=['rlc.reassembled.sn'])
                if len(gnb_iprlc_rel_df) == 0:
                    logger.error(f"Finding gnb_iprlc_rel_df was unsuccessful")
                    #print(gnb_iprlc_rel_df)
                    continue

                # For each pair of ['rlc.queue.R2buf', 'rlc.queue.sn'] in ue_ip_packets_df,
                # find corresponding entries in ue_rlc_segments_df with the same values for ['rlc.txpdu.R2buf', 'rlc.txpdu.sn']
                ue_ip_items_df["rlc.queue.sn"] = ue_ip_items_df["rlc.queue.sn"].astype(float)
                ue_ip_items_df["rlc.queue.R2buf"] = ue_ip_items_df["rlc.queue.R2buf"].astype(float)
                ue_rlc_items_df["rlc.txpdu.sn"] = ue_rlc_items_df["rlc.txpdu.sn"].astype(float)
                ue_rlc_items_df["rlc.txpdu.R2buf"] = ue_rlc_items_df["rlc.txpdu.R2buf"].astype(float)
                ue_ip_items_df["rlc.queue.sn"] = ue_ip_items_df["rlc.queue.sn"].astype(int)
                ue_ip_items_df["rlc.queue.R2buf"] = ue_ip_items_df["rlc.queue.R2buf"].astype(int)
                ue_rlc_items_df["rlc.txpdu.sn"] = ue_rlc_items_df["rlc.txpdu.sn"].astype(int)
                ue_rlc_items_df["rlc.txpdu.R2buf"] = ue_rlc_items_df["rlc.txpdu.R2buf"].astype(int)
                ue_iprlc_rel_df = pd.merge(ue_ip_items_df[['rlc.queue.R2buf', 'rlc.queue.sn',  'ip_id']],
                                        ue_rlc_items_df[['rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.srn','rlc.txpdu.timestamp', 'rlc.txpdu.length', 'txpdu_id']],  # Additional columns from ue_rlc_segments_df
                                        left_on=['rlc.queue.R2buf', 'rlc.queue.sn'],
                                        right_on=['rlc.txpdu.R2buf', 'rlc.txpdu.sn'])
                ue_iprlc_rel_df = ue_iprlc_rel_df.drop(columns=['rlc.queue.R2buf', 'rlc.queue.sn' , 'rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.timestamp', 'rlc.txpdu.length'])
                ue_iprlc_rel_df["ip_id"] = ue_iprlc_rel_df["ip_id"].astype(int)
                ue_ip_items_df["ip_id"] = ue_ip_items_df["ip_id"].astype(int)
                ue_rlc_items_df['txpdu_id'] = ue_rlc_items_df['txpdu_id'].astype(int)
                if len(ue_iprlc_rel_df) == 0:
                    logger.error(f"Finding ue_iprlc_rel_df was unsuccessful")
                    #print(ue_iprlc_rel_df)
                    continue

                packet_analyzer = ULPacketAnalyzer(upf_items_df, gnb_ip_items_df, gnb_rlc_items_df, gnb_iprlc_rel_df, gnb_mac_items_df, gnb_mcs_items_df, ue_ip_items_df, ue_rlc_items_df, ue_mac_items_df, ue_iprlc_rel_df)
                ue_srns_arr = list(range(int(packet_analyzer.first_rlcsrn), int(packet_analyzer.last_rlcsrn)))
                decomposed_packets_list = packet_analyzer.figure_packettx_from_ue_rlc_srn(ue_srns_arr, silent = True)
                flat_decomposed_packets_list = flatten_decomposed_packets(decomposed_packets_list)
                analyzed_packets_df = pd.DataFrame(flat_decomposed_packets_list)

                logger.debug(f"{analyzed_packets_df}")

                if analyzed_packets_df is not None:
                    stats_decomposed_packets += len(analyzed_packets_df)
                    if len(analyzed_packets_df)>0:
                        # print(df)
                        logger.debug(f"[combine journeys] Pushing {len(analyzed_packets_df)} packet records to the database")
                        # push df to influxdb
                        if influx_cli:
                            influx_cli.push_dataframe_list(
                                [
                                    {
                                        "df" : analyzed_packets_df,
                                        "point_name" : "packet_decomposed",
                                        "time_key" : "ip.in_t"
                                    }
                                ],
                                main_bucket
                            )
                            stats_published_packets += len(analyzed_packets_df)
                        else:
                            logger.warning(f"[combine journeys] Failed to push {len(analyzed_packets_df)} packet records to the database as influx cli is not setup.")


            else:
                missing = []
                if upf_items_df is None: missing.append("upf")
                if gnb_ip_items_df is None: missing.append("gnb_ip")
                if gnb_rlc_items_df is None: missing.append("gnb_rlc")
                if gnb_mac_items_df is None: missing.append("gnb_mac")
                if gnb_mcs_items_df is None: missing.append("gnb_mcs")
                if ue_ip_items_df is None: missing.append("ue_ip")
                if ue_rlc_items_df is None: missing.append("ue_rlc")
                if ue_mac_items_df is None: missing.append("ue_mac")

                if len(missing) < 8:
                    logger.warning(f"[packets_decompose] For full decomposition, dataframes {missing} are not available.")

        except Exception as ex:
            logger.error(f"[packets_decompose] {ex}")
            logger.warning(traceback.format_exc())

def queue_process(
            client_name, config, 
            rawdata_queue,
            sline_queue
        ):

    stats_dropped_lines = 0
    stats_rcv_lines = 0
    
    stats_published_upf_journeys = 0
    stats_published_ue_ip_packets = 0
    stats_published_ue_rlc_segments = 0
    stats_published_ue_mac_attempts = 0
    stats_published_gnb_ip_packets = 0
    stats_published_gnb_rlc_segments = 0
    stats_published_gnb_mac_attempts = 0
    stats_published_gnb_mcs_reports = 0

    start_time = time.time()

    if config["influx_token"]:
        influx_cli = InfluxClient(influx_db_address, config["influx_token"], org)
        logger.success(f"[{client_name} queue process] influxDB client initialized")
    else:
        influx_cli = None
        logger.error(f"[{client_name} queue process] influxDB client NONE")

    if client_name == 'UE':
        if (rawdata_queue is None):
            return
        while_sleep_time = QPROC_SLEEP_UE_S
        rdts = rdtsctotsOnline("UE")
        proc = ProcessULUE(
            enable_ip_packets = True,
            enable_rlc_segments = True,
            enable_mac_attempts = True,
            enable_uldcis_reports = False,
            enable_sched_reports = False,
            silent = True
        )
    elif client_name == 'GNB':
        if (rawdata_queue is None):
            return
        while_sleep_time = QPROC_SLEEP_GNB_S
        rdts = rdtsctotsOnline("GNB")
        proc = ProcessULGNB(
            enable_ip_packets = True,
            enable_rlc_segments = True,
            enable_sched_reports = False,
            enable_sched_maps = False,
            enable_rlc_reports = False,
            enable_mac_attempts = True,
            enable_mcs_reports = True,
            silent = True
        )
    elif client_name == 'UPF':
        if (rawdata_queue is None):
            return
        while_sleep_time = QPROC_SLEEP_UPF_S
        rdts = None
        proc = None

    logger.success(f"[{client_name} queue process] process starts.")

    raw_inputs = []
    while True:

        time.sleep(while_sleep_time)

        try:
            # check if we have a full page ready
            if rawdata_queue.get_length() == rawdata_queue.size:

                # get a copy of the page
                raw_inputs = rawdata_queue.get_items()

                # update slines if not UPF
                if client_name == 'GNB' or client_name == 'UE':
                    slines = sline_queue.get_items()
                    if slines:
                        rdts.s_lines = slines
                    else:
                        if len(rdts.s_lines) < 2:
                            logger.warning(f"[{client_name} queue process] waiting for CPU frequency info via S lines")
                            continue

                # update stats
                stats_rcv_lines = stats_rcv_lines + len(raw_inputs)
                if client_name == 'UE':

                    l1lines = rdts.return_rdtsctots(raw_inputs)
                    raw_inputs = []  
                    l1lines.reverse()
                    if len(l1lines) > 0:
                        ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df, _, _, _, _, _ = proc.run(l1lines)

                        publish_list = []
                        if len(ue_ip_packets_df)>0:
                            publish_list.append(
                                { "df" : ue_ip_packets_df, "point_name": "ue_ip", "time_key": "ip.in.timestamp" }
                            )
                            stats_published_ue_ip_packets += len(ue_ip_packets_df)

                        if len(ue_rlc_segments_df)>0:
                            publish_list.append(
                                { "df" : ue_rlc_segments_df, "point_name": "ue_rlc", "time_key": "rlc.txpdu.timestamp" }
                            )
                            stats_published_ue_rlc_segments += len(ue_rlc_segments_df)

                        if len(ue_mac_attempts_df)>0:
                            publish_list.append(
                                { "df" : ue_mac_attempts_df, "point_name": "ue_mac", "time_key": "mac.harq.timestamp" }
                            )
                            stats_published_ue_mac_attempts += len(ue_mac_attempts_df)

                        if publish_list:
                            influx_cli.push_dataframe_list(publish_list, raw_bucket)

                        publish_list = []
                        ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df = [], [], []
                        
                elif client_name == 'GNB':

                    l1lines = rdts.return_rdtsctots(raw_inputs)
                    raw_inputs = []  
                    if len(l1lines) > 0:
                        gnb_ip_packets_df, gnb_rlc_segments_df, _, _, _, gnb_mac_attempts_df, gnb_mcs_reports_df = proc.run(l1lines)

                        publish_list = []
                        if len(gnb_ip_packets_df)>0:
                            publish_list.append(
                                { "df" : gnb_ip_packets_df, "point_name": "gnb_ip", "time_key": "gtp.out.timestamp" }
                            )
                            stats_published_gnb_ip_packets += len(gnb_ip_packets_df)

                        if len(gnb_rlc_segments_df)>0:
                            publish_list.append(
                                { "df" : gnb_rlc_segments_df, "point_name": "gnb_rlc", "time_key": "rlc.reassembled.timestamp" }
                            )
                            stats_published_gnb_rlc_segments += len(gnb_rlc_segments_df)

                        if len(gnb_mac_attempts_df)>0:
                            publish_list.append(
                                { "df" : gnb_mac_attempts_df, "point_name": "gnb_mac", "time_key": "phy.detectstart.timestamp" }
                            )
                            stats_published_gnb_mac_attempts += len(gnb_mac_attempts_df)

                        if len(gnb_mcs_reports_df)>0:
                            publish_list.append(
                                { "df" : gnb_mcs_reports_df, "point_name": "gnb_mcs", "time_key": "timestamp" }
                            )
                            stats_published_gnb_mcs_reports += len(gnb_mcs_reports_df)

                        if publish_list:
                            influx_cli.push_dataframe_list(publish_list, raw_bucket)

                        publish_list = []
                        gnb_ip_packets_df, gnb_rlc_segments_df, gnb_mac_attempts_df, gnb_mcs_reports_df = [], [], [], []

                elif client_name == 'UPF':
                    upf_journeys = process_ul_nlmt(raw_inputs)

                    upf_items_df = pd.DataFrame(upf_journeys)
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
                        stats_published_upf_journeys += len(upf_items_df)

                    if publish_list:
                        influx_cli.push_dataframe_list(publish_list, raw_bucket)
                    
                    publish_list = []
                    upf_items_df = None
                    raw_inputs = []
                    upf_journeys = []

            # print stats
            current_time = time.time()
            elapsed_time = current_time - start_time
            if int(elapsed_time) >= LOGGING_PERIOD_SEC:
                if client_name == 'UE':
                    logger.info(f"[{client_name} queue process] received lines: {stats_rcv_lines}, dropped lines: {stats_dropped_lines}, published ue_ip_packets: {stats_published_ue_ip_packets}")
                    start_time = current_time
                elif client_name == 'GNB':
                    logger.info(f"[{client_name} queue process] received lines: {stats_rcv_lines}, dropped lines: {stats_dropped_lines}, published gnb_ip_packets: {stats_published_gnb_ip_packets}")
                    start_time = current_time
                elif client_name == 'UPF':
                    logger.info(f"[{client_name} queue process] received lines: {stats_rcv_lines}, dropped lines: {stats_dropped_lines}, published UPF journeys: {stats_published_upf_journeys}")
                    start_time = current_time

        except Exception as ex:
            logger.error(f"[{client_name} queue process] {ex}")
            logger.warning(traceback.format_exc())
            # update stats, clean the queues
            stats_dropped_lines = stats_dropped_lines + len(raw_inputs)
            raw_inputs = []
            publish_list = []
            upf_items_df = None
            ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df = [], [], []
            gnb_ip_packets_df, gnb_rlc_segments_df, gnb_mac_attempts_df, gnb_mcs_reports_df = [], [], [], []
            upf_journeys = []


async def handle_client(reader, writer, client_name, config, rawdata_queue, sline_queue):
    init = True
    rem_str = ''
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
                        if (client_name == 'UE' or client_name == 'GNB') and (' S ' in line): # check s line
                            sline_queue.append(line)
                        rawdata_queue.append(line)
                        stats_published_lines = stats_published_lines + 1
            else:
                if '\n' in message:
                    received_lines = message.splitlines()
                    received_lines[0] = rem_str + received_lines[0]
                    rem_str = ''
                    for line in received_lines[:-1]:
                        if line != 'test':
                            if (client_name == 'UE' or client_name == 'GNB') and (' S ' in line): # check s line
                                sline_queue.append(line)
                            rawdata_queue.append(line)
                            stats_published_lines = stats_published_lines + 1
                    rem_str = received_lines[-1]
                else:
                    rem_str = rem_str + message

            # print stats
            current_time = time.time()
            elapsed_time = current_time - start_time
            if int(elapsed_time) >= LOGGING_PERIOD_SEC:
                logger.info(f"[{client_name} server] published lines: {stats_published_lines}")
                start_time = current_time
            
    except asyncio.CancelledError:
        pass
    finally:
        logger.warning(f"[{client_name} server] Closing the connection")
        writer.close()

async def async_net_server(client_name, config, rawdata_queue, sline_queue):
    if rawdata_queue is None:
        return

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, client_name, config, rawdata_queue, sline_queue), 
        host='0.0.0.0', 
        port=config[client_name]["PORT"]
    )
    
    logger.success(f'[{client_name} server] serving on {server.sockets[0].getsockname()}')

    async with server:
        await server.serve_forever()

def net_server(client_name, config, rawdata_queue, sline_queue):
    asyncio.run(async_net_server(client_name, config, rawdata_queue, sline_queue))

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
    manager = Manager()

    #gnb_rawdata_queue = Queue(MAX_L1_GNB_DEPTH)
    gnb_rawdata_queue = SharedRingBuffer(size=MAX_L1_GNB_DEPTH, manager=manager)
    gnb_sline_queue = SharedRingBuffer(size=2, manager=manager)

    #ue_rawdata_queue = Queue(MAX_L1_UE_DEPTH)
    ue_rawdata_queue = SharedRingBuffer(size=MAX_L1_UE_DEPTH, manager=manager)
    ue_sline_queue = SharedRingBuffer(size=2, manager=manager)

    #upf_rawdata_queue = Queue(MAX_L1_UPF_DEPTH)
    upf_rawdata_queue = SharedRingBuffer(size=MAX_L1_UPF_DEPTH, manager=manager)

    try:
        # UPF
        upf_server = Process(target=net_server, args=("UPF", config, upf_rawdata_queue, None),daemon=True)
        upf_qprocess = Process(target=queue_process, args=("UPF", config, upf_rawdata_queue, None),daemon=True)

        # GNB
        gnb_server = Process(target=net_server, args=("GNB", config, gnb_rawdata_queue, gnb_sline_queue),daemon=True)
        gnb_qprocess = Process(target=queue_process, args=("GNB", config, gnb_rawdata_queue, gnb_sline_queue),daemon=True)

        # UE
        ue_server = Process(target=net_server, args=("UE", config, ue_rawdata_queue, ue_sline_queue),daemon=True)
        ue_qprocess = Process(target=queue_process, args=("UE", config, ue_rawdata_queue, ue_sline_queue),daemon=True)

        # COMBINE
        decompose_process = Process(target=packets_decompose, args=(config,), daemon=True)
        

        # start
        upf_server.start()
        upf_qprocess.start()

        gnb_server.start()
        gnb_qprocess.start()

        ue_server.start()
        ue_qprocess.start()

        decompose_process.start()

        # join
        upf_server.join()
        upf_qprocess.join()

        gnb_server.join()
        gnb_qprocess.join()

        ue_server.join()
        ue_qprocess.join()

        decompose_process.join()

    except KeyboardInterrupt:
        logger.warning("Caught KeyboardInterrupt, terminating workers")

        # terminate
        upf_server.terminate()
        upf_qprocess.terminate()

        gnb_server.terminate()
        gnb_qprocess.terminate()

        ue_server.terminate()
        ue_qprocess.terminate()
        
        decompose_process.terminate()
    else:
        logger.warning("Termination")

        # terminate
        upf_server.terminate()
        upf_qprocess.terminate()

        gnb_server.terminate()
        gnb_qprocess.terminate()

        ue_server.terminate()
        ue_qprocess.terminate()

        decompose_process.terminate()