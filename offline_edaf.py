import os, sys, gzip, json
from pathlib import Path
from loguru import logger
from collections import deque
import pandas as pd
from edaf.core.common.timestamp import rdtsctotsOnline
from edaf.core.uplink.gnb import ProcessULGNB
from edaf.core.uplink.ue import ProcessULUE
from edaf.core.uplink.nlmt import process_ul_nlmt
from edaf.core.uplink.combine import CombineUL
from edaf.core.uplink.decompose import process_ul_journeys
    
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

NUM_POP = 10000

# in case you have offline parquet journey files, you can use this script to decompose delay
# pass the address of a folder in argv with the following structure:
# FOLDER_ADDR/
# -- gnb/
# ---- latseq.*.lseq
# -- ue/
# ---- latseq.*.lseq
# -- upf/
# ---- se_*.json.gz

class RingBuffer:
    def __init__(self, size):
        self.size = size
        self.buffer = deque(maxlen=size)

    def append(self, item):
        if len(self.buffer) == self.size:
            logger.warning("RingBuffer is being overwritten. Consider increasing the buffer size.")
        self.buffer.append(item)

    def get_items(self):
        return list(self.buffer)

    def reverse_items(self):
        return list(reversed(self.buffer))

    def pop_items(self, n):
        popped_items = []
        for _ in range(min(n, len(self.buffer))):
            popped_items.append(self.buffer.popleft())
        return popped_items
        
    def get_length(self):
        return len(self.buffer)

if __name__ == "__main__":

    if len(sys.argv) != 3:
        logger.error("Usage: python offline_edaf.py <source_folder_address> <result_parquetfile>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    folder_path = Path(sys.argv[1])
    result_parquet_file = Path(sys.argv[2])

    gnb_path = folder_path.joinpath("gnb")
    gnb_lseq_file = list(gnb_path.glob("*.lseq"))[0]
    logger.info(f"found gnb lseq file: {gnb_lseq_file}")

    ue_path = folder_path.joinpath("ue")
    ue_lseq_file = list(ue_path.glob("*.lseq"))[0]
    logger.info(f"found ue lseq file: {ue_lseq_file}")

    upf_path = folder_path.joinpath("upf")
    upf_file = list(upf_path.glob("se_*.json.gz"))[0]
    logger.info(f"found upf json file: {upf_file}")

    gnbrdts = rdtsctotsOnline("GNB")
    gnbproc = ProcessULGNB()
    uerdts = rdtsctotsOnline("UE")
    ueproc = ProcessULUE()
    combineul = CombineUL(max_depth=NUM_POP)

    # NLMT
    with gzip.open(upf_file, 'rt', encoding='utf-8') as file:
        nlmt_journeys = json.load(file)['oneway_trips']
    logger.info(f"Loaded {len(nlmt_journeys)} NLMT trips")

    # GNB
    gnb_lseq_file = open(gnb_lseq_file, 'r')
    gnb_lines = gnb_lseq_file.readlines()
    l1linesgnb = gnbrdts.return_rdtsctots(gnb_lines)
    if len(l1linesgnb) > 0:
        gnb_journeys = gnbproc.run(l1linesgnb)
    logger.info(f"Loaded {len(gnb_journeys)} GNB trips")

    # UE
    ue_lseq_file = open(ue_lseq_file, 'r')
    ue_lines = ue_lseq_file.readlines()
    l1linesue = uerdts.return_rdtsctots(ue_lines)
    l1linesue.reverse()
    if len(l1linesue) > 0:
        ue_journeys = ueproc.run(l1linesue)
    logger.info(f"Loaded {len(ue_journeys)} UE trips")
    ue_journeys.reverse()

    ind = 0
    df = pd.DataFrame()
    while True:
        if ind+NUM_POP > len(nlmt_journeys) or ind+NUM_POP > len(nlmt_journeys) or ind+NUM_POP > len(nlmt_journeys):
            # process all the rest
            df_combined = combineul.run(
                nlmt_journeys[ind:-1],
                gnb_journeys[ind:-1],
                ue_journeys[ind:-1]
            )
        else:
            # process a batch of size NUM_POP
            df_combined = combineul.run(
                nlmt_journeys[ind:ind+NUM_POP],
                gnb_journeys[ind:ind+NUM_POP],
                ue_journeys[ind:ind+NUM_POP]
            )

        logger.info(f'Combined len: {len(df_combined)}')
        df_to_append = process_ul_journeys(df_combined)
        logger.info(f'Processed len: {len(df_to_append)}')
        df = pd.concat([df, df_to_append], ignore_index=True)
        
        ind += NUM_POP
        if ind > len(nlmt_journeys) or ind > len(nlmt_journeys) or ind > len(nlmt_journeys):
            break
    
    logger.info(f"Combines logs, created a df with {len(df)} entries.")
    df.to_parquet(result_parquet_file, engine='pyarrow')

