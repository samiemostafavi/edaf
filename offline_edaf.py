import os, sys, gzip, json
from pathlib import Path
from loguru import logger
import pandas as pd
from edaf.core.uplink.preprocess import preprocess_ul
from edaf.core.uplink.analyze import ULPacketAnalyzer
import sqlite3
    
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

# in case you have offline parquet journey files, you can use this script to decompose delay
# pass the address of a folder in argv with the following structure:
# FOLDER_ADDR/
# -- gnb/
# ---- latseq.*.lseq
# -- ue/
# ---- latseq.*.lseq
# -- upf/
# ---- se_*.json.gz

if __name__ == "__main__":

    if len(sys.argv) != 3:
        logger.error("Usage: python offline_edaf.py <source_folder_address> <result_parquetfile>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    folder_path = Path(sys.argv[1])
    result_database_file = Path(sys.argv[2])

    # GNB
    gnb_path = folder_path.joinpath("gnb")
    gnb_lseq_file = list(gnb_path.glob("*.lseq"))[0]
    logger.info(f"found gnb lseq file: {gnb_lseq_file}")
    gnb_lseq_file = open(gnb_lseq_file, 'r')
    gnb_lines = gnb_lseq_file.readlines()
    
    # UE
    ue_path = folder_path.joinpath("ue")
    ue_lseq_file = list(ue_path.glob("*.lseq"))[0]
    logger.info(f"found ue lseq file: {ue_lseq_file}")
    ue_lseq_file = open(ue_lseq_file, 'r')
    ue_lines = ue_lseq_file.readlines()

     # NLMT
    nlmt_path = folder_path.joinpath("upf")
    nlmt_file = list(nlmt_path.glob("se_*.json.gz"))[0]
    with gzip.open(nlmt_file, 'rt', encoding='utf-8') as file:
        nlmt_records = json.load(file)['oneway_trips']
    logger.info(f"found nlmt json file: {nlmt_file}")

    # Open a connection to the SQLite database
    conn = sqlite3.connect(result_database_file)
    # process the lines
    preprocess_ul(conn, gnb_lines, ue_lines, nlmt_records)
    # Close the connection when done
    conn.close()
    logger.success(f"Tables successfully saved to '{result_database_file}'.")

    # post process
    analyzer = ULPacketAnalyzer(result_database_file)
    uids_arr = [1977,1978,1979,1980,1981,1982]
    packets = analyzer.figure_packettx_from_ueipids(uids_arr)
    print(packets)