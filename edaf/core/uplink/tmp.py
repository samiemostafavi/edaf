else:

                                # In this section we try to find the following lines and store them in arrays
                                # bsr.upd lcid4.lcgid1.tot87.ENTno281.fm937.sl18
                                # bsr.tx lcid4.bsri8.tot82.fm937.sl17.ENTno281
                                # sr.trig lcid4.srid0.srbuf0.ENTno281.fm937.sl18
                                # sr.tx lcid4.srid0.srcnt0.fm931.sl17.ENTno267

                                # Find bsr.upd for this ul.dci
                                # bsr.upd lcid4.lcgid1.tot134.ENTno282.fm938.sl7
                                BSR_UPDs = []
                                BSRUPD_str = "bsr.upd"
                                for kd,prev_lkne in enumerate(prev_lines):
                                    if (BSRUPD_str in prev_lkne) and (uldci_entnop in prev_lkne):
                                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_lkne)
                                        lcid_match = re.search(r'lcid(\d+)', prev_lkne)
                                        lcgid_match = re.search(r'lcgid(\d+)', prev_lkne)
                                        len_match = re.search(r'tot(\d+)', prev_lkne)
                                        fm_match = re.search(r'fm(\d+)', prev_lkne)
                                        sl_match = re.search(r'sl(\d+)', prev_lkne)
                                        if timestamp_match and fm_match and sl_match and lcid_match and lcgid_match and len_match:
                                            timestamp = float(timestamp_match.group(1))
                                            fm_value = int(fm_match.group(1))
                                            sl_value = int(sl_match.group(1))
                                            lcid_value = int(lcid_match.group(1))
                                            lcgid_value = int(lcgid_match.group(1))
                                            len_value = int(len_match.group(1))
                                        else:
                                            logger.warning(f"[UE] For {BSRUPD_str}, could not find properties in line {line_number-kd-1}. Skipping this {BSRUPD_str}")
                                            continue

                                        logger.debug(f"[UE] Found '{BSRUPD_str}' and '{uldci_entnop}' in line {line_number-kd-1}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, len: {len_value}")
                                        bsrupd_dict = {
                                            'frame': fm_value,
                                            'slot': sl_value,
                                            'timestamp' : timestamp,
                                            'lcid' : lcid_value,
                                            'lcgid' : lcgid_value,
                                            'len' : len_value,
                                        }
                                        BSR_UPDs.append(bsrupd_dict)

                                # only pick the oldest non-zero BSR update
                                #selected_bsrupd = { 'timestamp': np.inf }
                                #bsrupd_found = False
                                #for bsrupd in BSR_UPDs:
                                #    if bsrupd['len'] > 0:
                                #        if selected_bsrupd['timestamp'] > bsrupd['timestamp']:
                                #            selected_bsrupd = bsrupd
                                #            bsrupd_found = True
                                #if not bsrupd_found:
                                #    selected_bsrupd = {}

                                # Find bsr.tx for this ul.dci
                                # bsr.tx lcid4.bsri8.tot82.fm937.sl17.ENTno281
                                BSR_TXs = []
                                BSRTX_str = "bsr.tx"
                                for kd,prev_lkne in enumerate(prev_lines):
                                    if (BSRTX_str in prev_lkne) and (uldci_entnop in prev_lkne):
                                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_lkne)
                                        lcid_match = re.search(r'lcid(\d+)', prev_lkne)
                                        bsri_match = re.search(r'bsri(\d+)', prev_lkne)
                                        len_match = re.search(r'tot(\d+)', prev_lkne)
                                        fm_match = re.search(r'fm(\d+)', prev_lkne)
                                        sl_match = re.search(r'sl(\d+)', prev_lkne)
                                        if timestamp_match and fm_match and sl_match and lcid_match and bsri_match and len_match:
                                            timestamp = float(timestamp_match.group(1))
                                            fm_value = int(fm_match.group(1))
                                            sl_value = int(sl_match.group(1))
                                            lcid_value = int(lcid_match.group(1))
                                            bsri_value = int(bsri_match.group(1))
                                            len_value = int(len_match.group(1))
                                        else:
                                            logger.warning(f"[UE] For {BSRTX_str}, could not find properties in line {line_number-kd-1}. Skipping this {BSRTX_str}")
                                            continue

                                        logger.debug(f"[UE] Found '{BSRTX_str}' and '{uldci_entnop}' in line {line_number-kd-1}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, bsri: {bsri_value}, len: {len_value}")
                                        bsrtx_dict = {
                                            'frame': fm_value,
                                            'slot': sl_value,
                                            'timestamp' : timestamp,
                                            'lcid' : lcid_value,
                                            'bsri' : bsri_value,
                                            'len' : len_value,
                                        }
                                        BSR_TXs.append(bsrtx_dict)
                                
                                # only pick the BSR tx occured right after the selected BSR update
                                #selected_bsrtx = { 'timestamp': np.inf }
                                #bsrtx_found = False
                                #for bsrtx in BSR_TXs:
                                #    if not bsrupd_found:
                                #        if bsrtx['len'] > 0:
                                #            if selected_bsrtx['timestamp'] > bsrtx['timestamp']:
                                #                selected_bsrtx = bsrtx
                                #                bsrtx_found = True
                                #    else:
                                #        if selected_bsrupd['timestamp'] <= bsrtx['timestamp'] and bsrtx['len'] > 0:
                                #            if selected_bsrtx['timestamp'] > bsrtx['timestamp']:
                                #                selected_bsrtx = bsrtx
                                #                bsrtx_found = True
                                #if not bsrtx_found:
                                #    selected_bsrtx = {}


                                # Find sr.tx for this ul.dci
                                SR_TXs = []
                                SRTX_str = "sr.tx"
                                # sr.tx lcid4.srid0.srcnt0.fm931.sl17.ENTno267
                                for kd,prev_lkne in enumerate(prev_lines):
                                    if (SRTX_str in prev_lkne) and (BSRTX_str not in prev_lkne) and (uldci_entnop in prev_lkne):
                                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_lkne)
                                        lcid_match = re.search(r'lcid(\d+)', prev_lkne)
                                        srid_match = re.search(r'srid(\d+)', prev_lkne)
                                        srcnt_match = re.search(r'srcnt(\d+)', prev_lkne)
                                        fm_match = re.search(r'fm(\d+)', prev_lkne)
                                        sl_match = re.search(r'sl(\d+)', prev_lkne)
                                        if timestamp_match and fm_match and sl_match and lcid_match and srid_match and srcnt_match:
                                            timestamp = float(timestamp_match.group(1))
                                            fm_value = int(fm_match.group(1))
                                            sl_value = int(sl_match.group(1))
                                            lcid_value = int(lcid_match.group(1))
                                            srid_value = int(srid_match.group(1))
                                            srcnt_value = int(srcnt_match.group(1))
                                        else:
                                            logger.warning(f"[UE] For {SRTX_str}, could not find properties in line {line_number-kd-1}. Skipping this {SRTX_str}")
                                            continue

                                        logger.debug(f"[UE] Found '{SRTX_str}' and '{uldci_entnop}' in line {line_number-kd-1}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, len: {len_value}")
                                        srtx_dict = {
                                            'frame': fm_value,
                                            'slot': sl_value,
                                            'timestamp' : timestamp,
                                            'lcid' : lcid_value,
                                            'srid' : srid_value,
                                            'srcnt' : srcnt_value,
                                        }
                                        SR_TXs.append(srtx_dict)
                                
                                # only pick the oldest SR Tx
                                #selected_srtx = { 'timestamp': np.inf }
                                #srtx_found = False
                                #for srtx in SR_TXs:
                                #    if selected_srtx['timestamp'] > srtx['timestamp']:
                                #        selected_srtx = srtx
                                #        srtx_found = True
                                #if not srtx_found:
                                #    selected_srtx = {}
                                    
                                # Find sr.trig for this ul.dci
                                SR_TRIGs = []
                                SRTRIG_str = "sr.trig"
                                # sr.trig lcid4.srid0.srbuf0.ENTno281.fm937.sl18 
                                for kd,prev_lkne in enumerate(prev_lines):
                                    if (SRTRIG_str in prev_lkne) and (uldci_entnop in prev_lkne):
                                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_lkne)
                                        lcid_match = re.search(r'lcid(\d+)', prev_lkne)
                                        srid_match = re.search(r'srid(\d+)', prev_lkne)
                                        srbuf_match = re.search(r'srbuf(\d+)', prev_lkne)
                                        fm_match = re.search(r'fm(\d+)', prev_lkne)
                                        sl_match = re.search(r'sl(\d+)', prev_lkne)
                                        if timestamp_match and fm_match and sl_match and lcid_match and srid_match and srbuf_match:
                                            timestamp = float(timestamp_match.group(1))
                                            fm_value = int(fm_match.group(1))
                                            sl_value = int(sl_match.group(1))
                                            lcid_value = int(lcid_match.group(1))
                                            srid_value = int(srid_match.group(1))
                                            srbuf_value = int(srbuf_match.group(1))
                                        else:
                                            logger.warning(f"[UE] For {SRTRIG_str}, could not find properties in line {line_number-kd-1}. Skipping this {SRTRIG_str}")
                                            continue

                                        logger.debug(f"[UE] Found '{SRTRIG_str}' and '{uldci_entnop}' in line {line_number-kd-1}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}, len: {len_value}")
                                        srtrig_dict = {
                                            'frame': fm_value,
                                            'slot': sl_value,
                                            'timestamp' : timestamp,
                                            'lcid' : lcid_value,
                                            'srid' : srid_value,
                                            'srbuf' : srbuf_value,
                                        }
                                        SR_TRIGs.append(srtrig_dict)
                                
                                # only pick the oldest SR Trig
                                #selected_srtrig = { 'timestamp': np.inf }
                                #srtrig_found = False
                                #for srtrig in SR_TRIGs:
                                #    if selected_srtrig['timestamp'] > srtrig['timestamp']:
                                #        selected_srtrig = srtrig
                                #        srtrig_found = True
                                #if not srtrig_found:
                                #    selected_srtrig = {}

                                # set resulting lists to the ul dci list
                                uldci_dict['bsr_upd'] = BSR_UPDs
                                uldci_dict['bsr_tx'] = BSR_TXs
                                uldci_dict['sr_trig'] = SR_TRIGs
                                uldci_dict['sr_tx'] = SR_TXs
                                #uldci_dict['bsr_upd'] = selected_bsrupd
                                #uldci_dict['bsr_tx'] = selected_bsrtx
                                #uldci_dict['sr_trig'] = selected_srtrig
                                #uldci_dict['sr_tx'] = selected_srtx