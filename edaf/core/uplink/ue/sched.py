import sys
import re
from loguru import logger
import pandas as pd

import os
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")


def find_uldci_reports(lines):

    uldcis = []
    for line_number, line in enumerate(lines):
        # Find all ul.dcis
        # ul.dci rntif58e.rbb0.rbs5.sb10.ss3.ENTno282.fm938.sl11.fmtx938.sltx17
        ULDCI_str = "ul.dci"
        if (ULDCI_str in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            rnti_match = re.search(r'rnti([0-9a-fA-F]{4})', line)
            rbb_match = re.search(r'rbb(\d+)', line)
            rbs_match = re.search(r'rbs(\d+)', line)
            sb_match = re.search(r'sb(\d+)', line)
            ss_match = re.search(r'ss(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            fmtx_match = re.search(r'fmtx(\d+)', line)
            sltx_match = re.search(r'sltx(\d+)', line)
            uldci_ent_match = re.search(r'ENTno(\d+)', line)
            if timestamp_match and fm_match and sl_match and fmtx_match and sltx_match and rnti_match and rbb_match and rbs_match and sb_match and ss_match and uldci_ent_match:
                timestamp = float(timestamp_match.group(1))
                rnti_value = rnti_match.group(1)
                rbb_value = int(rbb_match.group(1))
                rbs_value = int(rbs_match.group(1))
                sb_value = int(sb_match.group(1))
                ss_value = int(ss_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                uldci_ent_value = int(uldci_ent_match.group(1))
                fmtx_value = int(fmtx_match.group(1))
                sltx_value = int(sltx_match.group(1))
            else:
                logger.warning(f"[UE] For {ULDCI_str}, could not find properties in line {line_number}. Skipping this '{ULDCI_str}'")
                continue

            uldci_dict = {
                'rnti': rnti_value,
                'frame': fm_value,
                'slot': sl_value,
                'frametx': fmtx_value,
                'slottx': sltx_value,
                'timestamp' : timestamp,
                'rbb' : rbb_value,
                'rbs' : rbs_value,
                'sb' : sb_value,
                'ss' : ss_value,
                'uldci_ent' : uldci_ent_value,
            }
            logger.debug(f"[UE] Found '{ULDCI_str}' in line {line_number},{uldci_dict}")
            uldcis.append(uldci_dict)

    logger.info(f"Extracted {len(uldcis)} uldcis on UE.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(uldcis)
    return df



def find_sched_reports(lines):

    bsrupds = []
    bsrtxs = []
    srtrigs = []
    srtxs = []
    for line_number, line in enumerate(lines):
        # In this section we try to find the following lines and store them in arrays
        # bsr.upd lcid4.lcgid1.tot87.ENTno281.fm937.sl18
        # bsr.tx lcid4.bsri8.tot82.fm937.sl17.ENTno281
        # sr.trig lcid4.srid0.srbuf0.ENTno281.fm937.sl18
        # sr.tx lcid4.srid0.srcnt0.fm931.sl17.ENTno267

        # Find bsr.upd for this ul.dci
        # bsr.upd lcid4.lcgid1.tot134.ENTno282.fm938.sl7
        BSRUPD_str = "bsr.upd"
        if (BSRUPD_str in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            lcid_match = re.search(r'lcid(\d+)', line)
            lcgid_match = re.search(r'lcgid(\d+)', line)
            len_match = re.search(r'tot(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and fm_match and sl_match and lcid_match and lcgid_match and len_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                lcid_value = int(lcid_match.group(1))
                lcgid_value = int(lcgid_match.group(1))
                len_value = int(len_match.group(1))
            else:
                logger.warning(f"[UE] For {BSRUPD_str}, could not find properties in line {line_number}. Skipping this {BSRUPD_str}")
                continue

            logger.debug(f"[UE] Found '{BSRUPD_str}' in line {line_number}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, len: {len_value}")
            bsrupd_dict = {
                'frame': fm_value,
                'slot': sl_value,
                'timestamp' : timestamp,
                'lcid' : lcid_value,
                'lcgid' : lcgid_value,
                'len' : len_value,
            }
            bsrupds.append(bsrupd_dict)


        # Find bsr.tx
        # bsr.tx lcid4.bsri8.tot82.fm937.sl17.ENTno281
        BSRTX_str = "bsr.tx"
        if (BSRTX_str in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            lcid_match = re.search(r'lcid(\d+)', line)
            bsri_match = re.search(r'bsri(\d+)', line)
            len_match = re.search(r'tot(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and fm_match and sl_match and lcid_match and bsri_match and len_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                lcid_value = int(lcid_match.group(1))
                bsri_value = int(bsri_match.group(1))
                len_value = int(len_match.group(1))
            else:
                logger.warning(f"[UE] For {BSRTX_str}, could not find properties in line {line_number}. Skipping this {BSRTX_str}")
                continue

            logger.debug(f"[UE] Found '{BSRTX_str}' in line {line_number}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, bsri: {bsri_value}, len: {len_value}")
            bsrtx_dict = {
                'frame': fm_value,
                'slot': sl_value,
                'timestamp' : timestamp,
                'lcid' : lcid_value,
                'bsri' : bsri_value,
                'len' : len_value,
            }
            bsrtxs.append(bsrtx_dict)
                                
        # Find sr.tx
        SRTX_str = "sr.tx"
        # sr.tx lcid4.srid0.srcnt0.fm931.sl17.ENTno267
        if (SRTX_str in line) and (BSRTX_str not in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            lcid_match = re.search(r'lcid(\d+)', line)
            srid_match = re.search(r'srid(\d+)', line)
            srcnt_match = re.search(r'srcnt(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and fm_match and sl_match and lcid_match and srid_match and srcnt_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                lcid_value = int(lcid_match.group(1))
                srid_value = int(srid_match.group(1))
                srcnt_value = int(srcnt_match.group(1))
            else:
                logger.warning(f"[UE] For {SRTX_str}, could not find properties in line {line_number}. Skipping this {SRTX_str}")
                continue

            logger.debug(f"[UE] Found '{SRTX_str}' in line {line_number}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, len: {len_value}")
            srtx_dict = {
                'frame': fm_value,
                'slot': sl_value,
                'timestamp' : timestamp,
                'lcid' : lcid_value,
                'srid' : srid_value,
                'srcnt' : srcnt_value,
            }
            srtxs.append(srtx_dict)
                                
        # Find sr.trigs
        SRTRIG_str = "sr.trig"
        # sr.trig lcid4.srid0.srbuf0.ENTno281.fm937.sl18 
        if (SRTRIG_str in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            lcid_match = re.search(r'lcid(\d+)', line)
            srid_match = re.search(r'srid(\d+)', line)
            srbuf_match = re.search(r'srbuf(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and fm_match and sl_match and lcid_match and srid_match and srbuf_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                lcid_value = int(lcid_match.group(1))
                srid_value = int(srid_match.group(1))
                srbuf_value = int(srbuf_match.group(1))
            else:
                logger.warning(f"[UE] For {SRTRIG_str}, could not find properties in line {line_number}. Skipping this {SRTRIG_str}")
                continue

            logger.debug(f"[UE] Found '{SRTRIG_str}' in line {line_number}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, len: {len_value}")
            srtrig_dict = {
                'frame': fm_value,
                'slot': sl_value,
                'timestamp' : timestamp,
                'lcid' : lcid_value,
                'srid' : srid_value,
                'srbuf' : srbuf_value,
            }
            srtrigs.append(srtrig_dict)

    logger.info(f"Extracted {len(bsrupds)} BSR updates, {len(bsrtxs)} BSR txs, {len(srtrigs)} SR triggers, {len(srtxs)} SR txs on UE.")

    # Convert the list of dicts to a DataFrame
    return pd.DataFrame(bsrupds), pd.DataFrame(bsrtxs), pd.DataFrame(srtrigs), pd.DataFrame(srtxs)        



