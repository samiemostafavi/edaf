

def plot_packet_tree(packet, prev_input_ip_ts, ax):

    # Starting point
    x_start, y_start = 0, 0

    # plot
    if prev_input_ip_ts is 0:
        x_os = 0
    else:
        x_os = (packet['ip.in_t'] - prev_input_ip_ts)*1000
    x_start = x_start + x_os

    branch0_x = x_start + (packet['rlc.in_t'] - packet['ip.in_t'])*1000
    branch0_y = y_start + 1
    ax.plot([x_start, branch0_x], [y_start, branch0_y], color='green')

    # Iterate over each attempt
    num_rlc_attempts = len(packet['rlc.attempts'])
    for i in range(num_rlc_attempts):
        tx_pdu_timestamp = packet['rlc.attempts'][i]['mac.in_t']
        branch1_x = branch0_x + (tx_pdu_timestamp - packet['rlc.in_t'])*1000
        branch1_y = branch0_y + 1

        if not packet['rlc.attempts'][i]['acked']:
          # this rlc segment could be nacked, check it TODO
          # plot the rlc segment line
          ax.plot([branch0_x, branch1_x], [branch0_y, branch1_y], color='red')
          continue 
        else:
          # plot the rlc segment line
          ax.plot([branch0_x, branch1_x], [branch0_y, branch1_y], color='blue')

        for j in range(len(packet['rlc.attempts'][i]['mac.attempts'])):
            if packet['rlc.attempts'][i]['mac.attempts'][j]['acked']:
                # ue side mac
                branch2_x = branch1_x + (
                        packet['rlc.attempts'][i]['mac.attempts'][j]['phy.in_t']-tx_pdu_timestamp
                    )*1000
                branch2_y = branch1_y + 1
                ax.plot([branch1_x, branch2_x], [branch1_y, branch2_y], color='orange')

                # gnb/ue side phy
                branch3_x = branch2_x + (
                    packet['rlc.attempts'][i]['mac.attempts'][j]['phy.out_t'] - packet['rlc.attempts'][i]['mac.attempts'][j]['phy.in_t']
                )*1000
                branch3_y = branch2_y + 1
                ax.plot([branch2_x, branch3_x], [branch2_y, branch3_y], color='orange')

                # gnb side mac
                branch4_x = branch3_x + (
                        packet['rlc.attempts'][i]['mac.out_t']-packet['rlc.attempts'][i]['mac.attempts'][j]['phy.out_t']
                    )*1000
                branch4_y = branch3_y + 1
                ax.plot([branch3_x, branch4_x], [branch3_y, branch4_y], color='orange') 

            else:
                # only ue side, in grey
                branch2_x = branch1_x + (
                        packet['rlc.attempts'][i]['mac.attempts'][j]['phy.in_t']-packet['rlc.attempts'][i]['mac.in_t']
                    )*1000
                branch2_y = branch1_y + 1
                ax.plot([branch1_x, branch2_x], [branch1_y, branch2_y], color='red') 

    branch5_x = branch4_x + (packet['ip.out_t']-packet['rlc.out_t'])*1000
    branch5_y = branch4_y + 1
    ax.plot([branch4_x, branch5_x], [branch4_y, branch5_y], color='green')   

    return packet['ip.in_t']       



def plot_sched_tree(begin_ts, end_ts, ax):

    NUM_PRBS = 106
    SYMS_PER_SLOT = 14
    SLOTS_PER_FRAME = 20
    SLOT_LENGTH = 0.5 #ms
    MAX_QUEUE_SIZE = 1000

    #begin_ts = begin_ts - 0.005

    # bring all bsr.upd within this frame
    # find bsr updates transmitted 'bsr.tx'
    bsr_upd_list = ue_bsrupds_df[
        (ue_bsrupds_df['timestamp'] >= begin_ts - 0.005) &
        (ue_bsrupds_df['timestamp'] < end_ts)
    ]
    if bsr_upd_list.shape[0] == 0:
        logger.warning("Did not find any bsr report for this interval.")

    bsr_upd_rows = []
    for i in range(bsr_upd_list.shape[0]):
        bsr_upd_row = bsr_upd_list.iloc[i]
        bsr_upd_rows.append(bsr_upd_row)

    sorted_bsr_upd_rows = sorted(bsr_upd_rows, key=lambda x: x['timestamp'])

    for i in range(len(sorted_bsr_upd_rows)):
        bsr_upd_row = sorted_bsr_upd_rows[i]
        # 'frame', 'slot', 'timestamp', 'lcid', 'bsri', 'len'
        width = 10
        tot_height = 0.5
        full_segment_height = tot_height / MAX_QUEUE_SIZE * bsr_upd_row['len']
        empty_segment_height = tot_height - full_segment_height
        x_pos = (bsr_upd_row['timestamp'] - begin_ts)*1000
        y_pos = 1
        rect = patches.Rectangle(
            (
                x_pos, 
                y_pos,
            ),
            width, 
            full_segment_height, 
            color='red'
        )
        ax.add_patch(rect)
        rect = patches.Rectangle(
            (
                x_pos,
                y_pos+full_segment_height
            ),
            width, 
            empty_segment_height, 
            color='grey'
        )
        ax.add_patch(rect)


    # bring all bsr.tx within this frame
    # find bsr updates transmitted 'bsr.tx'
    bsrtx_list = ue_bsrtxs_df[
        (ue_bsrtxs_df['timestamp'] >= begin_ts - 0.005) &
        (ue_bsrtxs_df['timestamp'] < end_ts)
    ]
    if bsrtx_list.shape[0] == 0:
        logger.warning("Did not find any bsr report for this interval.")

    for i in range(bsrtx_list.shape[0]):
        bsrts_row = bsrtx_list.iloc[i]
        # 'frame', 'slot', 'timestamp', 'lcid', 'bsri', 'len'
        width = 0.1
        tot_height = 0.5
        full_segment_height = tot_height / MAX_QUEUE_SIZE * bsrts_row['len']
        empty_segment_height = tot_height - full_segment_height
        x_pos = (bsrts_row['timestamp'] - begin_ts)*1000
        y_pos = 1.5
        rect = patches.Rectangle(
            (
                x_pos, 
                y_pos,
            ),
            width, 
            full_segment_height, 
            color='red'
        )
        ax.add_patch(rect)
        rect = patches.Rectangle(
            (
                x_pos,
                y_pos+full_segment_height
            ),
            width, 
            empty_segment_height, 
            color='grey'
        )
        ax.add_patch(rect)


    # bring all sr.tx within this frame
    # find bsr updates transmitted 'sr.tx'
    srtx_list = ue_srtxs_df[
        (ue_srtxs_df['timestamp'] >= begin_ts - 0.005) &
        (ue_srtxs_df['timestamp'] < end_ts)
    ]
    if srtx_list.shape[0] == 0:
        logger.warning("Did not find any bsr report for this interval.")

    for i in range(srtx_list.shape[0]):
        srtx_row = srtx_list.iloc[i]
        # 'frame', 'slot', 'timestamp', 'lcid', 'bsri', 'len'
        width = 0.1
        height = 0.5
        x_pos = (srtx_row['timestamp'] - begin_ts)*1000
        y_pos = 1.5
        rect = patches.Rectangle(
            (
                x_pos, 
                y_pos,
            ),
            width, 
            height, 
            color='red'
        )
        ax.add_patch(rect)

    # bring all sched.ue within this frame
    sched_ue_list = gnb_sched_reports_df[
        (gnb_sched_reports_df['sched.ue.timestamp'] >= begin_ts-0.010) &
        (gnb_sched_reports_df['sched.ue.timestamp'] < end_ts)
    ]
    for i in range(sched_ue_list.shape[0]):
        # find sched.ue events
        ue_sched_row = sched_ue_list.iloc[i]
        ue_sched_ts = ue_sched_row['sched.ue.timestamp']

        # what is its cause?
        if ue_sched_row['sched.cause.type'] > 0:
            if int(ue_sched_row['sched.cause.type']) == 1:
                # due to bsr
                width = 0.1
                height = 0.5
                x_pos = (ue_sched_row['sched.ue.timestamp'] - begin_ts)*1000
                y_pos = 2
                rect = patches.Rectangle(
                    (
                        x_pos, 
                        y_pos,
                    ),
                    width, 
                    height, 
                    color='green'
                )
                ax.add_patch(rect)

            elif int(ue_sched_row['sched.cause.type']) == 2:
                # due to sr
                width = 0.1
                height = 0.5
                x_pos = (ue_sched_row['sched.ue.timestamp'] - begin_ts)*1000
                y_pos = 2
                rect = patches.Rectangle(
                    (
                        x_pos, 
                        y_pos,
                    ),
                    width, 
                    height, 
                    color='orange'
                )
                ax.add_patch(rect)
            elif int(ue_sched_row['sched.cause.type']) == 3:
                # no activity, we dont do anything for this
                # due to non activity
                width = 0.1
                height = 0.5
                x_pos = (ue_sched_row['sched.ue.timestamp'] - begin_ts)*1000
                y_pos = 2
                rect = patches.Rectangle(
                    (
                        x_pos, 
                        y_pos,
                    ),
                    width, 
                    height, 
                    color='yellow'
                )
                ax.add_patch(rect)

            ue_bsr_ts_no_os = (ue_sched_ts-begin_ts)*1000+SLOT_LENGTH*4
            width = 0.25
            height = 1
            # find fm, sl and fmtx, sltx
            abs_sltx_po = int(ue_sched_row[f'sched.ue.frametx'])*SLOTS_PER_FRAME +int(ue_sched_row[f'sched.ue.slottx'])
            abs_sl_po = int(ue_sched_row[f'sched.ue.frame'])*SLOTS_PER_FRAME +int(ue_sched_row[f'sched.ue.slot'])
            sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*SLOT_LENGTH
            x1_pos = x_pos
            y1_pos = 2.5
            x2_pos = ue_bsr_ts_no_os+sltx_tsdif_ms
            y2_pos = 3
            ax.plot([x1_pos, x2_pos], [y1_pos, y2_pos], color='orange') 
            

            # ul dci
            uldci_list = ue_uldcis_df[
                (ue_uldcis_df['timestamp'] >= begin_ts- 0.005) &
                (ue_uldcis_df['timestamp'] < end_ts)
            ]
            if uldci_list.shape[0] == 0:
                logger.warning("Did not find any uldci_list for this interval.")

            for i in range(uldci_list.shape[0]):
                uldci_row = uldci_list.iloc[i]
                # 'frame', 'slot', 'timestamp', 'lcid', 'bsri', 'len'
                width = 0.1
                height = 0.5
                x_pos = (uldci_row['timestamp'] - begin_ts)*1000
                y_pos = 4.5
                rect = patches.Rectangle(
                    (
                        x_pos, 
                        y_pos,
                    ),
                    width, 
                    height, 
                    color='blue'
                )
                ax.add_patch(rect)

                # find fm, sl and fmtx, sltx
                ue_bsr_ts_no_os = (uldci_row['timestamp']-begin_ts)*1000 #+SLOT_LENGTH*4
                abs_sltx_po = int(uldci_row['frametx'])*SLOTS_PER_FRAME +int(uldci_row['slottx'])
                abs_sl_po = int(uldci_row[f'frame'])*SLOTS_PER_FRAME +int(uldci_row[f'slot'])
                sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*SLOT_LENGTH
                x1_pos = x_pos
                y1_pos = 4.5
                x2_pos = ue_bsr_ts_no_os+sltx_tsdif_ms
                y2_pos = 4
                ax.plot([x1_pos, x2_pos], [y1_pos, y2_pos], color='orange')

    return


def plot_tree_from_sns(sn_list : list, ax):
   
    # find interval
    end_ts = 0
    begin_ts = np.inf
    input_ip_ts = 0
    for sn in sn_list:
        gnb_ip_row = gnb_ip_packets_df[gnb_ip_packets_df['gtp.out.sn'] == sn].iloc[0]
        filtered_df = gnb_iprlc_rel_df[gnb_iprlc_rel_df['gtp.out.sn'] == sn]
        gnb_rlc_rows = []
        for i in range(filtered_df.shape[0]):
            sdu_id = int(filtered_df.iloc[i]['sdu_id'])
            gnb_rlc_rows.append(gnb_rlc_segments_df[gnb_rlc_segments_df['sdu_id'] == sdu_id].iloc[0])

        filtered_df = ue_iprlc_rel_df[ue_iprlc_rel_df['rlc.txpdu.srn'] == sn]
        ip_id = int(filtered_df.iloc[0]['ip_id'])
        ue_ip_row = ue_ip_packets_df[ue_ip_packets_df['ip_id'] == ip_id].iloc[0]

        ue_rlc_rows = []
        for i in range(filtered_df.shape[0]):
            txpdu_id = filtered_df.iloc[i]['txpdu_id']
            ue_rlc_rows.append(ue_rlc_segments_df[ue_rlc_segments_df['txpdu_id'] == txpdu_id].iloc[0])

        input_ip_ts = plot_tree(gnb_ip_row, gnb_rlc_rows, ue_ip_row, ue_rlc_rows, input_ip_ts, ax)

        end_ts = max(end_ts,gnb_ip_row['gtp.out.timestamp'])
        begin_ts = min(begin_ts,ue_ip_row['ip.in.timestamp'])

    return begin_ts,end_ts
