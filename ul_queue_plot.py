import sys
import pandas as pd
from loguru import logger
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib

MAX_SEGMENTS = 20
MAX_HQROUND = 5
RU_LATENCY_MS = 0.5

quantiles = [0.999]
files = [
    '240103_122733_FINAL_expB_Q0_results/journeys.parquet',
    '240103_011728_FINAL_expB_Q1_results/journeys.parquet',
    '240103_130202_FINAL_expB_Q2_results/journeys.parquet',
    '240103_134147_FINAL_expB_Q3_results/journeys.parquet',
    '240103_141622_FINAL_expB_Q4_results/journeys.parquet',
    '240103_145232_FINAL_expB_Q5_results/journeys.parquet',
    '240103_152707_FINAL_expB_Q6_results/journeys.parquet',
    '240103_155807_FINAL_expB_Q7_results/journeys.parquet',
    '240103_162609_FINAL_expB_Q8_results/journeys.parquet'
]

arrivals_start = 0 #ms
arrivals_end = 10 #ms
arrivals_window_size = 1 #ms
arrivals_windows_step = 0.125 #ms

def generate_window_intervals():
    intervals = []
    current_start = arrivals_start
    while current_start + arrivals_window_size <= arrivals_end:
        intervals.append([current_start, current_start + arrivals_window_size])
        current_start += arrivals_windows_step
    return intervals
offset_intervals = generate_window_intervals()
logger.info(f"Arrival times windows: {offset_intervals}")

import scienceplots
plt.style.use(['science','ieee'])

def read_parquet_file(file_path):
    try:
        logger.info(f"Openning {file_path}...")
        # Read the Parquet file into a Pandas DataFrame
        df = pd.read_parquet(file_path, engine='pyarrow')

    except FileNotFoundError:
        logger.error(f"Error: File '{file_path}' not found.")
    except pd.errors.EmptyDataError:
        logger.error(f"Error: The Parquet file '{file_path}' is empty.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    return df

class TDDSchedule:
    frame_length = 10 # ms
    slot_dur = 0.5 # ms
    ul_slots = [7,8,9,17,18,19]
    ul_slots_ref_ts = None
    ul_slots_avg_ts = None
    tdd_ts_offset_ms = None

    def get_ref_ts(self, slot: int):

        found = False
        for idx,ms in enumerate(self.ul_slots):
            if ms == slot:
                found = True
                if self.ul_slots_ref_ts is not None:
                    return self.ul_slots_ref_ts[idx]
                else:
                    logger.error(f"Ref timestamps are not initialized.")
                    return None

        if not found:
            logger.error(f"Ref timestamp not found for {slot} in {self.ul_slots}.")
            return None

    def get_avg_ts(self, slot: int):

        found = False
        for idx,ms in enumerate(self.ul_slots):
            if ms == slot:
                found = True
                if self.ul_slots_avg_ts is not None:
                    return self.ul_slots_avg_ts[idx]
                else:
                    logger.error(f"Avg timestamps are not initialized.")
                    return None

        if not found:
            logger.error(f"Avg timestamp not found for {slot} in {self.ul_slots}.")
            return None

    def __init__(self,df):
        # Calculate 5G TDD Frames Time Offset
        logger.info(f"Calculating timestamps of {self.frame_length} ms TDD frames, {self.ul_slots} as uplink slots, and slot duration of {self.slot_dur} ms.")

        timestamp_str = "rlc.reassembled.0.mac.demuxed.timestamp"
        rx_tss_fn = np.floor(df[timestamp_str] * 100)
        tx_tss_ms = list( (df[timestamp_str] * 1000) - (rx_tss_fn*10) )
        df['rx_ts_ms'] = tx_tss_ms

        # Add a new column: tdd_ts_offset_ms
        def get_tdd_ts_offset_ms(row):
            slot_num = row['rlc.reassembled.0.mac.demuxed.slot']
            # 0.2 ms delay for demodulation and decoding
            slot_ref_offset = slot_num*self.slot_dur+RU_LATENCY_MS

            tdd_offset_ms = slot_ref_offset - row['rx_ts_ms']
            return tdd_offset_ms
        # Apply the function to filter rows
        df['tdd_ts_offset_ms'] = df.apply(get_tdd_ts_offset_ms, axis=1)


if __name__ == "__main__":

    
    columns = [str(num) for num in quantiles]
    columns.append('avg')
    e2e_df = pd.DataFrame(columns=columns)
    q_df = pd.DataFrame(columns=['avg'])

    for ind, file in enumerate(files):
        if ind == 0:
            df = read_parquet_file(file)
            # Remove the first and last N elements to filter nonsense
            N = 10
            df = df.iloc[N:-N]
            # Reset the indices
            df = df.reset_index(drop=True)
        else:
            newdf = read_parquet_file(file)
            # Remove the first and last N elements to filter nonsense
            N = 10
            newdf = newdf.iloc[N:-N]
            # Reset the indices
            newdf = newdf.reset_index(drop=True)
            df = pd.concat([df,newdf], ignore_index=True)

    logger.info(f"Loaded {len(df)} journeys.")
    tdd_schedule = TDDSchedule(df)

    logger.info(f"Calculating arrival times")
    # Radio Arrival Time
    timestamp_str = 'ip.in.timestamp'
    # Add the tdd sync offset to all timestamps
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    df['arrival_ref_time'] = tx_tss_fn*10
    # Calculate ms offset within the frame
    tx_tss_ms = list((df[timestamp_str] * 1000) - (tx_tss_fn*10))
    df['radio_arrival_time_os'] = tx_tss_ms

    logger.info(f"Calculating e2e delays")
    # End-to-End Delay
    timestamp1 = df["receive.timestamp"]
    timestamp2 = df["send.timestamp"]
    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = ((timestamp1 - timestamp2) * 1000) #-32.0
    df['e2e_delay'] = timestamp_difference

    logger.info(f"Calculating queuing delay")
    # Queuing Delay
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.queue.timestamp"]
    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000
    df['queuing_delay'] = timestamp_difference

    # Service Time
    timestamp_str = 'rlc.queue.segments.0.rlc.txpdu.timestamp'     
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
    df['service_time_os'] = tx_tss_ms
    df['service_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    # Radio Departure Time
    timestamp_str = 'rlc.reassembled.0.mac.demuxed.timestamp'
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
    df['radio_departure_time_os'] = tx_tss_ms
    df['radio_departure_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    for ind,os_interval in enumerate(offset_intervals):
        # filter arrivals
        mask = ((df['radio_arrival_time_os'] >= os_interval[0]) & (df['radio_arrival_time_os'] <= os_interval[1]))
        filtered_df = df[mask]
        logger.info(f"Number of packets after applying {os_interval} as the arrival offset filter: {len(filtered_df)}")

        values = list(filtered_df['e2e_delay'].quantile(quantiles))
        values.append(filtered_df['e2e_delay'].mean())
        e2e_df.loc[ind] = values

        q_df.loc[ind] = filtered_df['queuing_delay'].mean()

    colors = plt.cm.tab10(np.array([0,1,2,3,4,5,6]))

    xticks = [ (item[0]+item[1])/2.0 for item in offset_intervals ]
    fig, ax = plt.subplots(1,1,figsize=(3,2))
    e2e_list = np.transpose(np.array(e2e_df.values.tolist()))
    labels = [('E2E delay ' + str(item)+' quantile') for item in quantiles]
    labels = [*labels,'E2E delay average']
    for idx,e2e_line in enumerate(e2e_list):
        logger.info(f"Drawing {labels[idx]}")
        ax.plot(xticks,e2e_line,label=labels[idx],marker='o',markersize=3,markevery=3,color=colors[idx])
    q_list = np.transpose(np.array(q_df.values.tolist()))
    logger.info(f"Drawing queuing delay")
    ax.plot(xticks,q_list[0],label='Queuing delay average',marker='x',markersize=3,markevery=3,color=colors[len(e2e_list)])
    ax.legend(loc='center right')
    ax.set_xlim([0+arrivals_window_size/2.0,10-arrivals_window_size/2.0])
    ax.set_ylim([0,20])
    ax.set_xticks(np.array(range(9))+1)
    # Plot histogram
    ax.set_ylabel("Delay [ms]")
    ax.set_xlabel("Relative packet arrival time [ms]")
    ax.grid()

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig('queue_results/queue.png')

    #####################################################################################

    mask = ((df['radio_arrival_time_os'] >= 5.5) & (df['radio_arrival_time_os'] <= 6.5))
    filtered_df = df[mask]

    columns = 3
    rows = 1
    bins_width = 0.25 #ms
    slots_width = 0.5 #ms
    fig = plt.figure(figsize=(2.5/1.5*columns,2.5/1.5*rows)) #, constrained_layout=True
    gs = fig.add_gridspec(rows, hspace=0)
    ax = gs.subplots(sharex=True, sharey=True)

    def plot_time_row(k,axs):
        ax = axs[k]
        ax.set_xlim([0,(10*columns)])
        ax.set_yscale('log')
        ax.set_ylim([1e-4,10])

        for c in range(columns):
            ax.fill_between([c*10+8.5,c*10+10], 0, [10,10], alpha=0.3, color='orange', linewidth=0.0)
            ax.fill_between([c*10+3.5,c*10+5], 0, [10,10], alpha=0.3, color='orange', linewidth=0.0)

            # Draw vertical lines separating Frames
            if c!=0:
                ax.axvline(x=c*10, color='black', linestyle='-')

        return ax

    def finish_plot_time(ax):
        # Adjust layout
        #plt.tight_layout()
        ax.legend(loc='upper right')#,fontsize=7)
        ax.grid()

        # set y axis ticks and labels
        yticks = [1.e-03,1.e-02,1.e-01,1.e+00]
        ylabels = ['$10^{-3}$','$10^{-2}$','$10^{-1}$','$10^0$']
        ax.set_yticks(yticks)
        ax.set_yticklabels(ylabels, rotation=0)
        ax.set_ylim([1e-4,10])

    colors = plt.cm.tab10(np.array([0,1,2,3,4,5,6]))
    ax = plot_time_row(0,[ax])
    # arrivals
    tbins = np.arange(0, columns*10+bins_width, bins_width)
    ax.hist(filtered_df['radio_arrival_time_os'], bins=tbins, density=True, alpha=1, color=colors[0],label='Arrival times')
    ax.hist(filtered_df['service_time'], bins=tbins, density=True, alpha=1, color=colors[2],label='Service times')
    ax.hist(filtered_df['radio_departure_time'], bins=np.arange(0+RU_LATENCY_MS, columns*10+slots_width+RU_LATENCY_MS, slots_width), density=True, alpha=1, color=colors[1],label='Radio departures')
    ax.set_ylabel('Probability')
    finish_plot_time(ax)

    fig.subplots_adjust(hspace=0)

    # set bottom x-axis ticks and labels
    xticks = list(range(1,10*columns+1,2))
    ax.set_xticks(xticks)
    labels = []
    for num in xticks:
        res = (num%10)*2
        if num == 5:
            res = str(res) + f'\nSlots in frame n'
        elif (num%10)==5:
            rem = int(np.floor(num/10))
            res = str(res) + f'\nSlots in frame n+{rem}'
        else:
            res = str(res)
        labels.append(res)
    ax.set_xticklabels(labels, rotation=0)

    # set top x-axis ticks and labels
    ax2 = ax.twiny()
    ax2.set_xlim([0,(10*columns)])
    ax2.set_xticks(xticks)
    labels = [str(num) for num in xticks]
    ax2.set_xticklabels(labels, rotation=0)
    ax2.set_xlabel("Time [ms]")

    # Save the figure to a file
    plt.savefig('queue_results/timeplot_queue_A.png')


    #####################################################################################

    mask = ((df['radio_arrival_time_os'] >= 7) & (df['radio_arrival_time_os'] <= 8))
    filtered_df = df[mask]

    columns = 3
    rows = 1
    bins_width = 0.25 #ms
    slots_width = 0.5 #ms
    fig = plt.figure(figsize=(2.5/1.5*columns,2.5/1.5*rows)) #, constrained_layout=True
    gs = fig.add_gridspec(rows, hspace=0)
    ax = gs.subplots(sharex=True, sharey=True)

    colors = plt.cm.tab10(np.array([0,1,2,3,4,5,6]))
    ax = plot_time_row(0,[ax])
    # arrivals
    tbins = np.arange(0, columns*10+bins_width, bins_width)
    ax.hist(filtered_df['radio_arrival_time_os'], bins=tbins, density=True, alpha=1, color=colors[0],label='Arrival times')
    ax.hist(filtered_df['service_time'], bins=tbins, density=True, alpha=1, color=colors[2],label='Service times')
    ax.hist(filtered_df['radio_departure_time'], bins=np.arange(0+RU_LATENCY_MS, columns*10+slots_width+RU_LATENCY_MS, slots_width), density=True, alpha=1, color=colors[1],label='Radio departures')
    ax.set_ylabel('Probability')
    finish_plot_time(ax)

    fig.subplots_adjust(hspace=0)

    # set bottom x-axis ticks and labels
    xticks = list(range(1,10*columns+1,2))
    ax.set_xticks(xticks)
    labels = []
    for num in xticks:
        res = (num%10)*2
        if num == 5:
            res = str(res) + f'\nSlots in frame n'
        elif (num%10)==5:
            rem = int(np.floor(num/10))
            res = str(res) + f'\nSlots in frame n+{rem}'
        else:
            res = str(res)
        labels.append(res)
    ax.set_xticklabels(labels, rotation=0)

    # set top x-axis ticks and labels
    ax2 = ax.twiny()
    ax2.set_xlim([0,(10*columns)])
    ax2.set_xticks(xticks)
    labels = [str(num) for num in xticks]
    ax2.set_xticklabels(labels, rotation=0)
    ax2.set_xlabel("Time [ms]")

    # Save the figure to a file
    plt.savefig('queue_results/timeplot_queue_B.png')