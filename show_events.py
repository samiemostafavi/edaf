# prompt: open a .parquet file and load the database into a dataframe
from loguru import logger
import sqlite3, random
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from edaf.core.uplink.analyze import ULPacketAnalyzer
import pickle

# mac_attempt_event:
# acked: {0,1}
# is_retx: {0,1}
# size: {
#    0-50: 00,
#    50-100: 01,
#    100-150: 10,
#    150-inf: 11
#}
def convert_type_to_dict(res : int):
    m_dict = {}

    # Extract 'acked' (bit 0)
    m_dict['acked'] = bool(res & 0b0001)

    # Extract 'prev_id' (bit 1)
    m_dict['prev_id'] = bool(res & 0b0010)

    # Extract 'len' based on bits 2 and 3
    len_bits = res & 0b1100
    if len_bits == 0b1100:
        m_dict['len'] = 200
    elif len_bits == 0b1000:
        m_dict['len'] = 150
    elif len_bits == 0b0100:
        m_dict['len'] = 100
    else:
        m_dict['len'] = 50

    return m_dict

with open('pred.pkl', 'rb') as f:
    pred_data = pickle.load(f)['pred']

logger.info(f"Opened pred pkl file with {len(pred_data[0])} event sequences")
logger.info(f"Sequence lengths: {len(pred_data[0][0])}")

num_samples = len(pred_data[0])
for i in range(num_samples):

    fig, ax = plt.subplots()

    seq_times = pred_data[0][i]
    seq_events = pred_data[1][i]

    for time,event in zip(seq_times,seq_events):
        if event == 16:
            # padding
            continue
        
        item = convert_type_to_dict(int(event))
        item['time'] = time
        
        if item['acked']:
            if item['prev_id']:
                color = 'orange'
            else:   
                color = 'blue'
        else:
            color = 'red'

        x_loc = item['time']
        length = item['len']
        ax.plot([x_loc, x_loc], [0, length], color=color, linewidth=1)

    #print(events)
    #dataset.append(events)
    #if len(dataset) > max_samples:
    #    break
    # Set title, labels, and grid
    ax.set_title('MAC Attempts')
    ax.set_xlabel('Time [ms]')  # Corrected: ax.set_xlabel()
    ax.set_ylabel('Bytes')  # Corrected: ax.set_ylabel()
    #ax.set_ylim([0,6])
    #ax.set_xlim([-5,(end_ts - begin_ts)*1000 + 5])
    ax.grid(False)

    fig.savefig("mac_events_pred.png")
    input("")

    #exit(0)

