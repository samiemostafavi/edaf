import sys
import re
from loguru import logger
from edaf.core.common.utils import RingBuffer
from edaf.core.common.utils import flatten_dict
import pandas as pd

import os
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

# maximum number of lines to check
MAX_DEPTH = 500

def find_mac_successful_attempts(previous_lines : RingBuffer, lines, silent = False):

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    mac_attempts = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # we are looking for mac block exchenge attempts,
        # it can be a successful decoded:
        # phy.detectstart ::fm102.sl8.hqpid15.hqround0.Hbuf1527077280
        # phy.detectend suc1.fm102.sl8.hqpid15.hqround0.Hbuf1527077280.ptot559.pn284.pth50
        # OLD VERSION: phy.decodeend fm102.sl8.hqpid15.hqround0.Hbuf1527077280.rbb0.rbs5.tbs24.mcs9.sb10.ss101 
        # actually we don't know in OLD VERSION if it was successful or not
        # NEW VERSION: phy.decodeend suc1.fm162.sl18.hqpid12.hqround0.Hbuf171586176.rbb0.rbs5.tbs24.mcs9.rnti1234

        # or it can be unsuccessful detect:
        # phy.detectstart ::fm918.sl8.hqpid9.hqround0.Hbuf1527077280
        # phy.detectend suc0.fm918.sl8.hqpid9.hqround0.Hbuf1527077280.ptot277.pn277.pth50

        # or it can be unsuccessful decode (NEW VERSION):
        # phy.detectstart ::fm918.sl8.hqpid9.hqround0.Hbuf1527077280
        # phy.detectend suc1.fm918.sl8.hqpid9.hqround0.Hbuf1527077280.ptot277.pn277.pth50
        # phy.decodeend suc0.fm162.sl18.hqpid12.hqround0.Hbuf171586176.rbb0.rbs5.tbs24.mcs9.rnti1234

        # therefore we first look for 'phy.decodeend'
        # OLD VERSION: phy.decodeend fm102.sl8.hqpid15.hqround0.Hbuf1527077280.rbb0.rbs5.tbs24.mcs9.sb10.ss101
        # NEW VERSION: phy.decodeend suc1.fm162.sl18.hqpid12.hqround0.Hbuf171586176.rbb0.rbs5.tbs24.mcs9.rnti1234
        KW_MAC_DEC = 'phy.decodeend'
        if (KW_MAC_DEC in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            hqpid_match = re.search(r'hqpid(\d+)', line)
            hqround_match = re.search(r'hqround(\d+)', line)
            hbuf_match = re.search(r'Hbuf(\d+)', line)
            rbb_match = re.search(r'rbb(\d+)', line)
            rbs_match = re.search(r'rbs(\d+)', line)
            tbs_match = re.search(r'tbs(\d+)', line)
            mcs_match = re.search(r'mcs(\d+)', line)
            if timestamp_match and fm_match and sl_match and hqpid_match and hqround_match and hbuf_match and rbb_match and rbs_match and tbs_match and mcs_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                hqpid_value = int(hqpid_match.group(1))
                hqround_value = int(hqround_match.group(1))
                hbuf_value = int(hbuf_match.group(1))
                rbb_value = int(rbb_match.group(1))
                rbs_value = int(rbs_match.group(1))
                tbs_value = int(tbs_match.group(1))
                mcs_value = int(mcs_match.group(1))
                # NEW VERSION, backward compatibility
                rnti_match = re.search(r'rnti([0-9a-fA-F]+)', line)
                suc_match = re.search(r'suc(\d+)', line)
                if rnti_match and suc_match:
                    rnti_value = rnti_match.group(1)
                    suc_value = int(suc_match.group(1))
                else:
                    rnti_value = None
                    suc_value = None
            else:
                logger.warning(f"For {KW_MAC_DEC}, could not find properties in line {line_number}.")
                continue

            mac_dec_arr = {
                KW_MAC_DEC: {
                    'timestamp': timestamp,
                    'rbb': rbb_value,
                    'rbs': rbs_value,
                    'tbs': tbs_value,
                    'mcs': mcs_value,
                    'rnti' : rnti_value,
                    'suc' : suc_value,
                    'frame': fm_value,
                    'slot': sl_value,
                    'hqpid': hqpid_value,
                    'hqround': hqround_value
                }
            }
            logger.debug(f"Found '{KW_MAC_DEC}' in line {line_number}, {mac_dec_arr[KW_MAC_DEC]}")

            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # find 'phy.detectend' with suc1
            # phy.detectend suc1.fm102.sl8.hqpid15.hqround0.Hbuf1527077280.ptot559.pn284.pth50
            hbufstr = f'Hbuf{hbuf_value}'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            found_MAC_DETEND = False
            KW_MAC_DETEND = 'phy.detectend'
            for jd,prev_ljne in enumerate(prev_lines):
                if (KW_MAC_DETEND in prev_ljne) and (hbufstr in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    suc_match = re.search(r'suc(\d+)', prev_ljne)
                    ptot_match = re.search(r'ptot(\d+)', prev_ljne)
                    pn_match = re.search(r'pn(\d+)', prev_ljne)
                    pth_match = re.search(r'pth(\d+)', prev_ljne)
                    if timestamp_match and ptot_match and pn_match and pth_match and suc_match:
                        timestamp = float(timestamp_match.group(1))
                        suc_value = int(suc_match.group(1))
                        ptot_value = int(ptot_match.group(1))
                        pn_value = int(pn_match.group(1))
                        pth_value = int(pth_match.group(1))
                    else:
                        logger.warning(f"[GNB] For {KW_MAC_DETEND}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                        break
                    mac_dec_arr[KW_MAC_DETEND] = {
                        'timestamp' : timestamp,
                        'frame': fm_value,
                        'slot': sl_value,
                        'hqpid': hqpid_value,
                        'hqround': hqround_value,
                        'hbuf': hbuf_value,
                        'ptot' : ptot_value,
                        'pn' : pn_value,
                        'pth' : pth_value,
                        'suc' : suc_value,
                    }
                    logger.debug(f"[GNB] Found '{KW_MAC_DETEND}','{hbufstr}','{fmstr}','{slstr}' in line {line_number-jd-1}, {mac_dec_arr[KW_MAC_DETEND]}")
                    found_MAC_DETEND = True
                    break

            if not found_MAC_DETEND:
                logger.warning(f"[GNB] Could not find '{KW_MAC_DETEND}' before {line_number} for {KW_MAC_DEC}")
                mac_dec_arr[KW_MAC_DETEND] = {}

            # find 'phy.measure.rssi' for rssi, cqi measurements# phy.measure.rssi rnti3cde.rssi-138.rssi_digital44.n_rb_ul8.wband_cqi127.n0_power0.rx_power23584.frame984.slot18
            found_RSSI_VAL = False            
            KW_RSSI_VAL = 'phy.measure.rssi'
            KW_RSSI_DEC = 'phy.measure'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            for jd,prev_ljne in enumerate(prev_lines):
                if (KW_RSSI_VAL in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    rnti_match = re.search(r'rnti([0-9a-fA-F]+)', prev_ljne)
                    rssi_match = re.search(r'rssi(-?\d+)', prev_ljne)
                    rssiDigital_match = re.search(r'rssi_digital(\d+)', prev_ljne)
                    n_rb_ul_match = re.search(r'n_rb_ul(\d+)', prev_ljne)
                    wband_cqi_match = re.search(r'wband_cqi(\d+)', prev_ljne)
                    n0_power_match = re.search(r'n0_power(\d+)', prev_ljne)
                    rx_power_match = re.search(r'rx_power(\d+)', prev_ljne)
                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                    if timestamp_match and rssi_match and rssiDigital_match and fm_match and sl_match and n_rb_ul_match and wband_cqi_match and n0_power_match and rx_power_match:
                        timestamp = float(timestamp_match.group(1))
                        fm_value = int(fm_match.group(1))
                        sl_value = int(sl_match.group(1))
                        rnti_value = rnti_match.group(1)
                        rssi_value = int(rssi_match.group(1))
                        rssiDigital_value = int(rssiDigital_match.group(1))
                        n_rb_ul_value = int(n_rb_ul_match.group(1))
                        wband_cqi_value = int(wband_cqi_match.group(1))
                        n0_power_value = int(n0_power_match.group(1))
                        rx_power_value = int(rx_power_match.group(1))
                        
                        # Add the extracted values the mac_dec_array list
                        mac_dec_arr[KW_RSSI_DEC] = {
                            'timestamp' : timestamp,
                            'frame':fm_value,
                            'slot': sl_value,
                            'rssi': rssi_value,
                            'rssi_digital': rssiDigital_value,
                            'nRb': n_rb_ul_value,
                            'wband_cqi' : wband_cqi_value,
                            'n0_power' : n0_power_value,
                            'rx_power' : rx_power_value,
                            'rnti' : rnti_value,
                        }
                        found_RSSI_VAL = True
                        logger.debug(f"[GNB] Found '{KW_RSSI_VAL}' in line {line_number-jd-1}")
                        break
                    
            if not found_RSSI_VAL:
                logger.warning(f"[GNB] Could not find '{KW_RSSI_VAL}' before {line_number} for {KW_RSSI_DEC}")
                mac_dec_arr[KW_RSSI_DEC] = {}

                   
            # find 'phy.detectstart'
            hbufstr = f'Hbuf{hbuf_value}'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            found_MAC_DETSTART = False
            KW_MAC_DETSTART = 'phy.detectstart'
            for jd,prev_ljne in enumerate(prev_lines):
                if (KW_MAC_DETSTART in prev_ljne) and (hbufstr in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    if timestamp_match:
                        timestamp = float(timestamp_match.group(1))
                    else:
                        logger.warning(f"[GNB] For {KW_MAC_DETSTART}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                        break
                    mac_dec_arr[KW_MAC_DETSTART] = {
                        'timestamp' : timestamp,
                    }
                    logger.debug(f"[GNB] Found '{KW_MAC_DETSTART}' in line {line_number-jd-1}, {mac_dec_arr[KW_MAC_DETSTART]}")
                    found_MAC_DETSTART = True
                    break

            if not found_MAC_DETSTART:
                logger.warning(f"[GNB] Could not find '{KW_MAC_DETSTART}' before {line_number} for {KW_MAC_DEC}")
                mac_dec_arr[KW_MAC_DETSTART] = {
                    'timestamp' : None,
                }

                    

            mac_attempts.append(flatten_dict(mac_dec_arr))

    if not silent:
        logger.info(f"Extracted {len(mac_attempts)} successful mac attempts on GNB.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(mac_attempts)
    return df


def find_mac_failed_attempts(previous_lines : RingBuffer, lines, silent = False):
    
    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    mac_attempts = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        KW_MAC_DEC = 'phy.decodeend'

        # find 'phy.detectend' with 'suc0'
        # phy.detectend suc0.fm918.sl8.hqpid9.hqround0.Hbuf1527077280.ptot277.pn277.pth50
        KW_MAC_DETEND = 'phy.detectend'
        if (KW_MAC_DETEND in line) and ('suc0' in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            ptot_match = re.search(r'ptot(\d+)', line)
            pn_match = re.search(r'pn(\d+)', line)
            pth_match = re.search(r'pth(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            hqpid_match = re.search(r'hqpid(\d+)', line)
            hqround_match = re.search(r'hqround(\d+)', line)
            hbuf_match = re.search(r'Hbuf(\d+)', line)
            if timestamp_match and ptot_match and pn_match and pth_match and fm_match and sl_match and hqpid_match and hqround_match and hbuf_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                hqpid_value = int(hqpid_match.group(1))
                hqround_value = int(hqround_match.group(1))
                hbuf_value = int(hbuf_match.group(1))
                ptot_value = int(ptot_match.group(1))
                pn_value = int(pn_match.group(1))
                pth_value = int(pth_match.group(1))
            else:
                logger.warning(f"[GNB] For {KW_MAC_DETEND}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                continue
            mac_dec_arr = {
                KW_MAC_DEC : {
                    'timestamp': None,
                    'rbb': None,
                    'rbs': None,
                    'tbs': None,
                    'mcs': None,
                    'sb' : None,
                    'ss' : None
                },
                KW_MAC_DETEND : {
                    'timestamp' : timestamp,
                    'frame': fm_value,
                    'slot': sl_value,
                    'hqpid': hqpid_value,
                    'hqround': hqround_value,
                    'hbuf': hbuf_value,
                    'ptot' : ptot_value,
                    'pn' : pn_value,
                    'pth' : pth_value,
                    'suc' : 0,
                }
            }
            logger.debug(f"[GNB] Found '{KW_MAC_DETEND}' and suc0 in line {line_number}, {mac_dec_arr[KW_MAC_DETEND]}")


            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # find 'phy.detectstart'
            # phy.detectstart ::fm918.sl8.hqpid9.hqround0.Hbuf1527077280
            hbufstr = f'Hbuf{hbuf_value}'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            found_MAC_DETSTART = False
            KW_MAC_DETSTART = 'phy.detectstart'
            for jd,prev_ljne in enumerate(prev_lines):
                if (KW_MAC_DETSTART in prev_ljne) and (hbufstr in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    if timestamp_match:
                        timestamp = float(timestamp_match.group(1))
                    else:
                        logger.warning(f"[GNB] For {KW_MAC_DETSTART}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                        break
                    mac_dec_arr[KW_MAC_DETSTART] = {
                        'timestamp' : timestamp,
                    }
                    logger.debug(f"[GNB] Found '{KW_MAC_DETSTART}' in line {line_number-jd-1}, {mac_dec_arr[KW_MAC_DETSTART]}")
                    found_MAC_DETSTART = True
                    break

            if not found_MAC_DETSTART:
                logger.warning(f"[GNB] Could not find '{KW_MAC_DETSTART}' before {line_number} for {KW_MAC_DEC}")
                mac_dec_arr[KW_MAC_DETSTART] = {
                    'timestamp' : None,
                }
                continue

            mac_attempts.append(flatten_dict(mac_dec_arr))

    if not silent:
        logger.info(f"Extracted {len(mac_attempts)} failed mac attempts on GNB.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(mac_attempts)
    return df

def find_rssi_values(previous_lines : RingBuffer, lines, silent = False):

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    rssiVal = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        KW_RSSI_DEC = 'rssiVal'

        # find line starting with 'PHY'
        # phy.measure.rssi rnti3cde.rssi-138.rssi_digital44.n_rb_ul8.wband_cqi127.n0_power0.rx_power23584.fm984.sl18
        KW_RSSI_VAL = 'phy.measure.rssi'
        if (KW_RSSI_VAL in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            rnti_match = re.search(r'rnti([0-9a-fA-F]+)', line)
            rssi_match = re.search(r'rssi(-?\d+)', line)
            rssiDigital_match = re.search(r'rssi_digital(\d+)', line)
            n_rb_ul_match = re.search(r'n_rb_ul(\d+)', line)
            wband_cqi_match = re.search(r'wband_cqi(\d+)', line)
            n0_power_match = re.search(r'n0_power(\d+)', line)
            rx_power_match = re.search(r'rx_power(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and rssi_match and rssiDigital_match and fm_match and sl_match and n_rb_ul_match and wband_cqi_match and n0_power_match and rx_power_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                rnti_value = rnti_match.group(1)
                rssi_value = int(rssi_match.group(1))
                rssiDigital_value = int(rssiDigital_match.group(1))
                n_rb_ul_value = int(n_rb_ul_match.group(1))
                wband_cqi_value = int(wband_cqi_match.group(1))
                n0_power_value = int(n0_power_match.group(1))
                rx_power_value = int(rx_power_match.group(1))                
            else:
                logger.warning(f"[GNB] For {KW_RSSI_VAL}, could not find properties in line {line_number-1}. Skipping this '{KW_RSSI_DEC}'")
                continue
            rssi_dec_arr = {
                KW_RSSI_DEC : {
                    'timestamp' : timestamp,
                    'frame': fm_value,
                    'slot': sl_value,
                    'rssi': rssi_value,
                    'rssi_digital': rssiDigital_value,
                    'nRb': n_rb_ul_value,
                    'wband_cqi' : wband_cqi_value,
                    'n0_power' : n0_power_value,
                    'rx_power' : rx_power_value,
                    'rnti' : rnti_value,
                }
            }
            logger.debug(f"[GNB] Found '{KW_RSSI_DEC}' in line {line_number}, {rssi_dec_arr[KW_RSSI_DEC]}")

            rssiVal.append(flatten_dict(rssi_dec_arr))

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(rssiVal)
    return df


def find_ulcqi_values(previous_lines : RingBuffer, lines, silent = False):
    
    ulcqi_values = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # find 'phy.measure.ulcqi' for crc_rssi, ul_cqi measurements
        # 174909208664010951 U phy.measure.ulcqi rssi992.ul_cqi187.hqpid1.CC_idP0.gnb_mod_idP0.fm857.sl18.len24.ta31.rnti3cde       
        KW_UL_CQI_VAL = 'phy.measure.ulcqi'
        KW_UL_CQI_DEC = 'phy2mac.measure'
        if (KW_UL_CQI_VAL in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            rnti_match = re.search(r'rnti([0-9a-fA-F]+)', line)
            crc_rssi_match = re.search(r'rssi(-?\d+)', line)
            hqpid_match = re.search(r'hqpid(\d+)', line)
            ul_cqi_match = re.search(r'ul_cqi(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and crc_rssi_match and hqpid_match and fm_match and sl_match and ul_cqi_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                rnti_value = rnti_match.group(1)
                crc_rssi_value = int(crc_rssi_match.group(1))
                hqpid_value = int(hqpid_match.group(1))
                ul_cqi_value = int(ul_cqi_match.group(1))
            else:
                logger.warning(f"[GNB] For {KW_UL_CQI_VAL}, could not find properties in line {line_number-1}. Skipping this '{KW_UL_CQI_DEC}'")
                continue

            # Add the extracted values the mac_dec_array list
            ulcqi_arr = {
                KW_UL_CQI_DEC : {
                    'timestamp' : timestamp,
                    'frame': fm_value,
                    'slot': sl_value,
                    'crc_rssi': crc_rssi_value,
                    'hqpid': hqpid_value,
                    'ul_cqi': ul_cqi_value,
                    'rnti' : rnti_value,
                }
            }

                      
            # lets go back in lines
            prev_lines = previous_lines.reverse_items()
            
            # Find if the mac retransmission was successful or not
            # For this, find the following line
            # phy.decodeend suc1.fm162.sl18.hqpid12.hqround0.Hbuf171586176.rbb0.rbs5.tbs24.mcs9.rnti1234
            KW_PHY_DEC = 'phy.decodeend'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            hqpidstr = f'hqpid{hqpid_value}'
            found_ulcqi_val = False
            for jd,prev_line in enumerate(prev_lines):
                if (KW_PHY_DEC in prev_line) and (fmstr in prev_line) and (slstr in prev_line) and (hqpidstr in prev_line):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                    rnti_match = re.search(r'rnti([0-9a-fA-F]+)', prev_line)
                    suc_match = re.search(r'suc(\d+)', prev_line)
                    hqpid_match = re.search(r'hqpid(\d+)', prev_line)
                    fm_match = re.search(r'fm(\d+)', prev_line)
                    sl_match = re.search(r'sl(\d+)', prev_line)
                    if timestamp_match and suc_match and hqpid_match and fm_match and sl_match:
                        timestamp = float(timestamp_match.group(1))
                        fm_value = int(fm_match.group(1))
                        sl_value = int(sl_match.group(1))
                        rnti_value = rnti_match.group(1)
                        suc_value = int(suc_match.group(1))
                        hqpid_value = int(hqpid_match.group(1))
                        found_ulcqi_val = True
                        break
                    else:
                        logger.warning(f"[GNB] For {KW_PHY_DEC}, could not find properties in line {line_number-1}. Skipping this '{KW_PHY_DEC}'")
                        continue
            
            if not found_ulcqi_val:
                ulcqi_arr[KW_UL_CQI_DEC] = {}
            
            ulcqi_values.append(flatten_dict(ulcqi_arr))
                                     

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(ulcqi_values)
    return df

            # if not found_CRC_RSSI_VAL:
            #     logger.warning(f"[GNB] Could not find '{KW_CRC_RSSI_VAL}' before {line_number} for {KW_CRC_RSSI_DEC}")
            #     mac_dec_arr[KW_CRC_RSSI_DEC] = {}

def find_rsrp_values(previous_lines : RingBuffer, lines, silent = False):

    rsrp_values = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # find 'phy.measure.rsrp' for rsrp measurements
        # 174909209485186107 U phy.measure.rsrp rsrp-94.rnti3cde.fm892.sl4  
        # 
        # ? : Why slot is always 4 in snr and rsrp measurements     
        KW_RSRP_VAL = 'phy.measure.rsrp'
        KW_RSRP_DEC = 'phy.rsrp_measure'
        # found_rsrp_val = False
        if (KW_RSRP_VAL in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            rnti_match = re.search(r'rnti([0-9a-fA-F]+)', line)
            rsrp_match = re.search(r'rsrp(-?\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and rsrp_match and fm_match and sl_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                rnti_value = rnti_match.group(1)
                rsrp_value = int(rsrp_match.group(1))
                # found_rsrp_val = True
            else:
                logger.warning(f"[GNB] For {KW_RSRP_VAL}, could not find properties in line {line_number-1}. Skipping this '{KW_RSRP_DEC}'")
                continue

            # Add the extracted values the rsrp_arr list
            rsrp_arr = {
                KW_RSRP_DEC : {
                    'timestamp' : timestamp,
                    'frame': fm_value,
                    'slot': sl_value,
                    'rsrp': rsrp_value,
                    'rnti' : rnti_value,
                }
            }           
        
            rsrp_values.append(flatten_dict(rsrp_arr))
    
    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(rsrp_values)
    return df

def find_snr_values(previous_lines : RingBuffer, lines, silent = False):

    snr_values = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # find 'phy.measure.snr' for snr measurements
        # 174909209485172091 U phy.measure.snr fm892.sl4.snr10  
        # 
        # ? : Why slot is always 4 in snr and rsrp measurements     
        KW_SNR_VAL = 'phy.measure.snr'
        KW_SNR_DEC = 'phy.snr_measure'
        if (KW_SNR_VAL in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            snr_match = re.search(r'snr(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            if timestamp_match and snr_match and fm_match and sl_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                snr_value = int(snr_match.group(1))
            else:
                logger.warning(f"[GNB] For {KW_SNR_VAL}, could not find properties in line {line_number-1}. Skipping this '{KW_SNR_DEC}'")
                continue

            # Add the extracted values the snr_arr list
            snr_arr = {
                KW_SNR_DEC : {
                    'timestamp' : timestamp,
                    'frame': fm_value,
                    'slot': sl_value,
                    'snr': snr_value,
                }
            }           
        
            snr_values.append(flatten_dict(snr_arr))
    
    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(snr_values)
    return df

