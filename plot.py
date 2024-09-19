# prompt: open a .parquet file and load the database into a dataframe
from loguru import logger
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from edaf.core.uplink.analyze import ULPacketAnalyzer


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

analyzer = ULPacketAnalyzer('database2.db')

fig, ax = plt.subplots()

uids_arr = [1977,1978,1979,1980,1981,1982,1983,1984,1985,1986]
packets = analyzer.figure_packettx_from_ueipids(uids_arr)
begin_ts = packets[0]['ip.in_t']
end_ts = packets[-1]['ip.out_t']

prev_packet_ipin_ts = 0
for packet in packets:
    if prev_packet_ipin_ts == 0:
        prev_packet_ipin_ts = plot_packet_tree(packet, prev_packet_ipin_ts, ax)
    else:
        plot_packet_tree(packet, prev_packet_ipin_ts, ax)

# Set title, labels, and grid
ax.set_xlabel('Time [ms]')  # Corrected: ax.set_xlabel()
ax.set_ylabel('Scheduling Process')  # Corrected: ax.set_ylabel()
ax.set_ylim([0,6])
ax.set_xlim([0,(end_ts - begin_ts)*1000])
ax.grid(False)

fig.savefig("res1.png")

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

