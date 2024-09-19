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
def convert_dict_to_type(m_dict):
    res = 0b0000

    if m_dict['acked']:
        res = res | 0b0001
    
    if m_dict['prev_id']:
        res = res | 0b0010

    if int(m_dict['len']) >= 150:
        res = res | 0b1100
    elif int(m_dict['len']) < 150 and int(m_dict['len']) >= 100:
        res = res | 0b1000
    elif int(m_dict['len']) < 100 and int(m_dict['len']) >= 50:
        res = res | 0b0100

    return int(res)


analyzer = ULPacketAnalyzer('database2.db')
tot_size = 2339

#uids_arr = [1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989]
X_LOC_FAC = 20
ueipid_len = 15
max_samples = np.inf
max_time = 0
dataset = []
for i in range(tot_size):
    if i < 100 or i >(tot_size-100):
        continue

    uids_arr = list(range(i, i + ueipid_len))
    #print(uids_arr)
    packets = analyzer.figure_packettx_from_ueipids(uids_arr)

    macattempts_dict = {}
    for packet in packets:
        rlcattempts = packet['rlc.attempts']
        for rlcatt in rlcattempts:
            macattempts = rlcatt['mac.attempts']
            for macatt in macattempts:
                macattempts_dict[int(macatt['id'])] = macatt

    
    macattempts = [value for value in macattempts_dict.values()]
    sorted_macattempts = sorted(macattempts, key=lambda x: x['phy.in_t'])

    #begin_ts = packets[0]['ip.in_t']
    #end_ts = packets[-1]['ip.out_t']

    #fig, ax = plt.subplots()

    #for packet in packets:
    #    x_loc = (packet['ip.in_t'] - begin_ts)*1000
    #    length = packet['len']
    #    color = 'green'
    #    ax.plot([x_loc, x_loc], [0, length], color=color, linewidth=1)
    #    x_loc = (packet['ip.out_t'] - begin_ts)*1000
    #    color = 'black'
    #    ax.plot([x_loc, x_loc], [0, length], color=color, linewidth=1)

    begin_ts = sorted_macattempts[0]['phy.in_t']
    end_ts = sorted_macattempts[-1]['phy.in_t']

    # format:
    # {'idx_event': 1, 'type_event': 8, 'time_since_start': 0.0, 'time_since_last_event': 0.0}
    events = []
    id_count = 0
    last_x_loc = 0
    for item in sorted_macattempts:
        
        if item['acked']:
            if item['prev_id']:
                color = 'orange'
            else:   
                color = 'blue'
        else:
            color = 'red'

        x_loc = (item['phy.in_t'] - begin_ts)*1000
        max_time = max(x_loc,max_time)
        #length = item['len']
        #ax.plot([x_loc, x_loc], [0, length], color=color, linewidth=1)

        event = {
            'idx_event' : id_count,
            'type_event' : convert_dict_to_type(item),
            'time_since_start' : x_loc/X_LOC_FAC,
            'time_since_last_event' : (x_loc-last_x_loc)/X_LOC_FAC,
        }
        events.append(event)
        last_x_loc = x_loc
        id_count = id_count + 1

    #print(events)
    dataset.append(events)
    if len(dataset) > max_samples:
        break
    # Set title, labels, and grid
    #ax.set_title('MAC Attempts')
    #ax.set_xlabel('Time [ms]')  # Corrected: ax.set_xlabel()
    #ax.set_ylabel('Bytes')  # Corrected: ax.set_ylabel()
    #ax.set_ylim([0,6])
    #ax.set_xlim([-5,(end_ts - begin_ts)*1000 + 5])
    #ax.grid(False)

    #fig.savefig("mac_events.png")
    #input("")

    #exit(0)

# split dataset
# train, dev, test
split_ratios = [0.7,0.15,0.15]
# shuffle
random.shuffle(dataset)

# print length of dataset
print(len(dataset))

# split
train_num = int(len(dataset)*split_ratios[0])
dev_num = int(len(dataset)*split_ratios[1])
print("train: ", train_num, " - dev: ", dev_num)
# train
train_ds = {
    'dim_process' : 16,
    'train' : dataset[0:train_num],
}
# Save the dictionary to a pickle file
with open('train.pkl', 'wb') as f:
    pickle.dump(train_ds, f)
# dev
dev_ds = {
    'dim_process' : 16,
    'dev' : dataset[train_num:train_num+dev_num],
}
# Save the dictionary to a pickle file
with open('dev.pkl', 'wb') as f:
    pickle.dump(dev_ds, f)
# test
test_ds = {
    'dim_process' : 16,
    'test' : dataset[train_num+dev_num:-1],
}
# Save the dictionary to a pickle file
with open('test.pkl', 'wb') as f:
    pickle.dump(test_ds, f)


exit(0)
# Now plot Rerouce blocks on top
plot_resourcegrid(begin_ts, end_ts, ax)

plot_sched_tree(begin_ts, end_ts, ax)

# Set title, labels, and grid
ax.set_xlabel('Time [ms]')  # Corrected: ax.set_xlabel()
ax.set_ylabel('Scheduling Process')  # Corrected: ax.set_ylabel()
ax.set_ylim([0,6])
ax.set_xlim([0,(end_ts - begin_ts)*1000])
ax.grid(False)

fig.savefig("res1.png")


fig, ax = plt.subplots()

begin_ts,end_ts = plot_packet_tree_from_ueipids(uids_arr, ax, True)

plot_resourcegrid(begin_ts, end_ts, ax)

# Set title, labels, and grid
ax.set_xlabel('Time [ms]')  # Corrected: ax.set_xlabel()
ax.set_ylabel('Packet Transmission Process')  # Corrected: ax.set_ylabel()
ax.set_ylim([0,6])
ax.set_xlim([0,(end_ts - begin_ts)*1000])
ax.grid(False)

fig.savefig("res2.png")

