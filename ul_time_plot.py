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

import scienceplots
plt.style.use(['science','ieee'])

def read_parquet_file(file_path):
    try:
        # Read the Parquet file into a Pandas DataFrame
        df = pd.read_parquet(file_path, engine='pyarrow')

        # Log the column names
        logger.info("Column Names:")
        for col in df.columns:
            logger.info(col)

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
    ul_slots = [7,8,9,17,18,19] #[7,8,9,17,18,19]
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
    # Check if the file name is provided as a command-line argument
    if len(sys.argv) != 3:
        logger.error("Usage: python script_name.py <parquet_file> <output_figure_file>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    file_path = sys.argv[1]

    # Call the function
    df = read_parquet_file(file_path)

    # Remove the first and last N elements to filter nonsense
    N = 10
    df = df.iloc[N:-N]
    # Reset the indices
    df = df.reset_index(drop=True)

    # Add a new column: rlc.reassembled.num_segments
    def get_num_segments(row):
        last_seg = 0
        for i in range(MAX_SEGMENTS):
            if f'rlc.queue.segments.{i}.rlc.txpdu.timestamp' in row:
                if row[f'rlc.queue.segments.{i}.rlc.txpdu.timestamp'] > 0:
                    last_seg = i
        return last_seg
    df['rlc.reassembled.num_segments'] = df.apply(get_num_segments, axis=1)

    # Add a new column: first_hqround_rx_ts
    def get_first_hqround_rx_ts(row):
        hqround_value = row[f'rlc.reassembled.0.mac.demuxed.hqround']
        decoded_slot_column = f'rlc.reassembled.0.mac.demuxed.mac.decoded.{hqround_value}.timestamp'
        return row[decoded_slot_column]
    # Apply the function to filter rows
    df['rlc.reassembled.0.mac.demuxed.mac.decoded.first.timestamp'] = df.apply(get_first_hqround_rx_ts, axis=1)

    # Add a new row: last_segment_txpdu_ts
    def get_last_segment_txpdu_ts(row):
        last_seg = 0
        for i in range(MAX_SEGMENTS):
            if f'rlc.queue.segments.{i}.rlc.txpdu.timestamp' in row:
                if row[f'rlc.queue.segments.{i}.rlc.txpdu.timestamp'] > 0:
                    last_seg = i
        last_seg_column = f'rlc.queue.segments.{last_seg}.rlc.txpdu.timestamp'
        return row[last_seg_column]
    # Apply the function to filter rows
    df['rlc.queue.segments.last.rlc.txpdu.timestamp'] = df.apply(get_last_segment_txpdu_ts, axis=1)

    tdd_schedule = TDDSchedule(df)

    ################### Radio Arrival Time ###################

    timestamp_str = 'ip.in.timestamp'

    # Add the tdd sync offset to all timestamps
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)

    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    df['arrival_ref_time'] = tx_tss_fn*10

    # Calculate ms offset within the frame
    tx_tss_ms = list((df[timestamp_str] * 1000) - (tx_tss_fn*10))
    df['radio_arrival_time_os'] = tx_tss_ms

    # filter arrival times
    #mask = ((df['radio_arrival_time_os'] >= 5) & (df['radio_arrival_time_os'] <= 6.5))
    #df = df[mask]

    ################### Service Time ###################

    timestamp_str = 'rlc.queue.segments.0.rlc.txpdu.timestamp'
    # Add the tdd sync offset to all timestamps        
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
    df['service_time_os'] = tx_tss_ms
    df['service_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    timestamp_str = 'rlc.queue.segments.1.rlc.txpdu.timestamp'
    if timestamp_str in df:
        # Add the tdd sync offset to all timestamps        
        df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
        # Calculate frame number
        tx_tss_fn = np.floor(df[timestamp_str] * 100)
        # Calculate ms offset within the frame
        tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
        df['service_time_seg2_os'] = tx_tss_ms
        df['service_time_seg2'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    timestamp_str = 'rlc.queue.segments.2.rlc.txpdu.timestamp'
    if timestamp_str in df:
        # Add the tdd sync offset to all timestamps        
        df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
        # Calculate frame number
        tx_tss_fn = np.floor(df[timestamp_str] * 100)
        # Calculate ms offset within the frame
        tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
        df['service_time_seg3_os'] = tx_tss_ms
        df['service_time_seg3'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    timestamp_str = 'rlc.queue.segments.3.rlc.txpdu.timestamp'
    if timestamp_str in df:
        # Add the tdd sync offset to all timestamps        
        df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
        # Calculate frame number
        tx_tss_fn = np.floor(df[timestamp_str] * 100)
        # Calculate ms offset within the frame
        tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
        df['service_time_seg4_os'] = tx_tss_ms
        df['service_time_seg4'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    ################### Radio Departure Time ###################

    timestamp_str = 'rlc.reassembled.0.mac.demuxed.timestamp'

    # Add the tdd sync offset to all timestamps
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)

    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)

    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )

    df['radio_departure_time_os'] = tx_tss_ms
    df['radio_departure_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']


    ################### Core Departure Time ###################
    timestamp_str = 'receive.timestamp'

    # Add the tdd sync offset to all timestamps
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
    df['core_departure_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    #####################################################################################

    columns = 3
    rows = 3
    bins_width = 0.25 #ms
    slots_width = 0.5 #ms
    fig = plt.figure(figsize=(2.5/1.5*columns,2/1.5*rows)) #, constrained_layout=True
    gs = fig.add_gridspec(rows, hspace=0)
    axs = gs.subplots(sharex=True, sharey=True)
    axs = axs.flatten()

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

    # arrivals
    ax = plot_time_row(0,axs)
    tbins = np.arange(0, columns*10+bins_width, bins_width)
    ax.hist(df['radio_arrival_time_os'], bins=tbins, density=True, alpha=1, color=colors[0],label='Arrival times')
    finish_plot_time(ax)

    # service Times
    ax = plot_time_row(1,axs)
    sum_counts = 0
    (counts1, bins1) = np.histogram(df['service_time'], bins=tbins)
    sum_counts += sum(counts1)
    logger.info(f'First segment service times: {sum(counts1)}')
    if 'service_time_seg2' in df:
        (counts2, bins2) = np.histogram(df['service_time_seg2'], bins=tbins)
        logger.info(f'Second segment service times: {sum(counts2)}')
        sum_counts += sum(counts2)
    if 'service_time_seg3' in df:
        (counts3, bins3) = np.histogram(df['service_time_seg3'], bins=tbins)
        logger.info(f'Third segment service times: {sum(counts3)}')
        sum_counts += sum(counts3)
    if 'service_time_seg4' in df:
        (counts4, bins4) = np.histogram(df['service_time_seg4'], bins=tbins)
        logger.info(f'Fourth segment service times: {sum(counts4)}')
        sum_counts += sum(counts4)

    factor = 1.0/sum_counts/bins_width
    ax.hist(bins1[:-1], bins1, weights=factor*counts1, alpha=0.8, color=colors[2],label='1st seg. service times')
    if 'service_time_seg2' in df:
        ax.hist(bins2[:-1], bins2, weights=factor*counts2, alpha=0.8, color=colors[4],label='2nd seg. service times')
    if 'service_time_seg3' in df:
        ax.hist(bins3[:-1], bins3, weights=factor*counts3, alpha=0.8, color=colors[3],label='3rd seg. service times')
    if 'service_time_seg4' in df:
        ax.hist(bins4[:-1], bins4, weights=factor*counts4, alpha=0.8, color=colors[5],label='4rd seg. service times')
    
    finish_plot_time(ax)

    # radio and core departures
    ax = plot_time_row(2,axs)
    ax.hist(df['radio_departure_time'], bins=np.arange(0+RU_LATENCY_MS, columns*10+slots_width+RU_LATENCY_MS, slots_width), density=True, alpha=1, color=colors[1],label='Radio departures')
    #ax.hist(df['core_departure_time'],bins=500, density=True, alpha=0.75, color=colors[0],label='Core departures')
    finish_plot_time(ax)

    fig.subplots_adjust(hspace=0)
    fig.supylabel('Probability')

    # set bottom x-axis ticks and labels
    ax = axs[-1]
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
    ax = axs[0]
    ax2 = ax.twiny()
    ax2.set_xlim([0,(10*columns)])
    ax2.set_xticks(xticks)
    labels = [str(num) for num in xticks]
    ax2.set_xticklabels(labels, rotation=0)
    ax2.set_xlabel("Time [ms]")

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'timeplot.png')



    #####################################################################################

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
    ax.hist(df['radio_arrival_time_os'], bins=tbins, density=True, alpha=1, color=colors[0],label='Arrival times')
    # service Times
    factor = 1.0/sum_counts/bins_width
    ax.hist(bins1[:-1], bins1, weights=factor*counts1, alpha=0.8, color=colors[2],label='Service times seg. 1')
    if 'service_time_seg2' in df:
        ax.hist(bins2[:-1], bins2, weights=factor*counts2, alpha=0.8, color=colors[4],label='Service times seg. 2')
    #if 'service_time_seg3' in df:
    #    ax.hist(bins3[:-1], bins3, weights=factor*counts3, alpha=0.8, color=colors[3],label='3rd seg. service times')
    #if 'service_time_seg4' in df:
    #    ax.hist(bins4[:-1], bins4, weights=factor*counts4, alpha=0.8, color=colors[5],label='4rd seg. service times')
    
    # radio and core departures
    ax.hist(df['radio_departure_time'], bins=np.arange(0+RU_LATENCY_MS, columns*10+slots_width+RU_LATENCY_MS, slots_width), density=True, alpha=1, color=colors[1],label='Radio departures')
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
    plt.savefig(Path(sys.argv[2])/'timeplot_compact.png')
