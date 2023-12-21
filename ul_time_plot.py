import sys
import pandas as pd
from loguru import logger
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

MAX_SEGMENTS = 20

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

        self.ul_slots_ref_ts = [ slot_num*self.slot_dur-1.0 for slot_num in self.ul_slots ]
        logger.info(f"Reference timestamps: {self.ul_slots_ref_ts} in ms")

        ul_slots_tss = [ [] for _ in self.ul_slots ]
        #out_slots = list(df["rlc.queue.segments.0.mac.sdu.slot"])
        #out_tss = list(df["rlc.queue.segments.0.mac.sdu.timestamp"])
        out_slots = list(df["rlc.reassembled.0.mac.demuxed.slot"])
        out_tss = list(df["rlc.reassembled.0.mac.demuxed.timestamp"])
        test = []
        test2 = []
        for idx, out_ts in enumerate(out_tss):
            out_ts_ms = (out_ts * 1000) - (np.floor(out_ts * 100)*10)
            test.append(out_ts_ms)
            test2.append(out_slots[idx])
            if int(out_slots[idx]) in self.ul_slots:
                idy = self.ul_slots.index(int(out_slots[idx]))
                out_ts_ms = (out_ts * 1000) - (np.floor(out_ts * 100)*10)
                ul_slots_tss[idy].append(out_ts_ms)

        self.ul_slots_avg_ts = []
        for out_ts_mss in ul_slots_tss:
            if len(out_ts_mss) != 0:
                self.ul_slots_avg_ts.append(np.mean(out_ts_mss))
            else:
                self.ul_slots_avg_ts.append(np.NaN)
        
        #remove NaNs
        nonan_res = []
        nonan_ul_slots = []
        nonan_ul_slots_offsets = []
        for idx,item in enumerate(self.ul_slots_avg_ts):
            if not np.isnan(item):
                nonan_res.append(item)
                nonan_ul_slots.append(self.ul_slots[idx])
                nonan_ul_slots_offsets.append(self.ul_slots_ref_ts[idx])

        logger.info(f"Blocks scheduled on {nonan_ul_slots}, with timestamps averaged: {nonan_res}")
        #tmp = np.array(nonan_res) - np.array(nonan_ul_slots_offsets)
        tmp = np.array(nonan_ul_slots_offsets) - np.array(nonan_res)
        for idx,item in enumerate(tmp):
            if item < 0:
                tmp[idx] = tmp[idx] + 10
        self.tdd_ts_offset_ms = np.mean(np.array(tmp))
        logger.info(f"Difference array: {tmp}")
        logger.info(f"Estimated offset between {nonan_ul_slots_offsets} and {nonan_res}: {self.tdd_ts_offset_ms} ms")


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

    # Create subplots
    fig, axs = plt.subplots(1, 4, figsize=(15, 2.5))
    axs = axs.flatten()
    axnum = 0

    ################### Radio Arrival Time ###################
    df['ip.in.timestamp'] = df['ip.in.timestamp'] + (tdd_schedule.tdd_ts_offset_ms/1000.0)
    tx_tss_ms = list( (df['ip.in.timestamp'] * 1000) - (np.floor(df['ip.in.timestamp'] * 100)*10) )
    tx_tss_fn = np.floor(df['ip.in.timestamp'] * 100)*10


    df['radio_arrival_time_os'] = tx_tss_ms
    smallest_frame = np.min(tx_tss_fn)
    df['radio_arrival_time_fn'] = (tx_tss_fn-smallest_frame)/10.0

    # Plot histogram
    ax = axs[axnum]
    ax.set_yscale('log')
    ax.set_xlim([0,9])
    ax.hist(tx_tss_ms, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Radio arrival times")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Time [ms]")
    axnum = axnum+1

    ################### Service Time ###################
    df['rlc.queue.segments.0.rlc.txpdu.timestamp'] = df['rlc.queue.segments.0.rlc.txpdu.timestamp'] + (tdd_schedule.tdd_ts_offset_ms/1000.0)
    tx_tss_ms = list( (df['rlc.queue.segments.0.rlc.txpdu.timestamp'] * 1000) - (np.floor(df['rlc.queue.segments.0.rlc.txpdu.timestamp'] * 100)*10) )
    tx_tss_fn = np.floor(df['rlc.queue.segments.0.rlc.txpdu.timestamp'] * 100)*10

    df['service_time_os'] = tx_tss_ms
    df['service_time_fn'] = (tx_tss_fn-smallest_frame)/10.0
    df['service_time'] = ((df['service_time_fn']-df['radio_arrival_time_fn'])*10)+df['service_time_os']

    # Plot histogram
    ax = axs[axnum]
    ax.set_yscale('log')
    ax.set_xlim([0,9])
    ax.hist(tx_tss_ms, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Service times")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Time [ms]")
    axnum = axnum+1

    ################### Radio Departure Time ###################
    df["rlc.reassembled.0.mac.demuxed.timestamp"] = df["rlc.reassembled.0.mac.demuxed.timestamp"] + (tdd_schedule.tdd_ts_offset_ms/1000.0)
    #out_tss = list(df["rlc.queue.segments.0.mac.sdu.timestamp"])
    tx_tss_ms = list( (df["rlc.reassembled.0.mac.demuxed.timestamp"] * 1000) - (np.floor(df["rlc.reassembled.0.mac.demuxed.timestamp"] * 100)*10) )
    tx_tss_fn = np.floor(df["rlc.reassembled.0.mac.demuxed.timestamp"] * 100)*10
    #out_tss_ms = []
    #out_tss_fn = []
    #for idx, out_ts in enumerate(out_tss):
    #    (q,r) = divmod((((out_ts * 1000) - (np.floor(out_ts * 100)*10)) + tdd_schedule.tdd_ts_offset_ms),10)
    #    out_tss_ms.append(r)
    #    out_tss_fn.append((np.floor(out_ts * 100)*10) + q)

    df['radio_departure_time_os'] = tx_tss_ms
    df['radio_departure_time_fn'] = (tx_tss_fn-smallest_frame)/10.0
    df['radio_departure_time'] = ((df['radio_departure_time_fn']-df['radio_arrival_time_fn'])*10)+df['radio_departure_time_os']

    # Plot histogram
    ax = axs[axnum]
    ax.set_yscale('log')
    ax.set_xlim([0,9])
    ax.hist(tx_tss_ms, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Radio Departure Times")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Offset [ms]")
    axnum = axnum+1

    ################### Core Departure Time ###################
    #out_tss = list(df["rlc.queue.segments.0.mac.sdu.timestamp"])
    df["receive.timestamp"] = df["receive.timestamp"] + (tdd_schedule.tdd_ts_offset_ms/1000.0)
    out_tss = np.array(list(df["receive.timestamp"]))
    out_tss_ms = []
    out_tss_fn = []
    for idx, out_ts in enumerate(out_tss):
        q,r = divmod((((out_ts * 1000) - (np.floor(out_ts * 100)*10)) + tdd_schedule.tdd_ts_offset_ms),10)
        out_tss_ms.append(r)
        out_tss_fn.append((np.floor(out_ts * 100)*10) + q)

    df['core_departure_time_os'] = out_tss_ms
    df['core_departure_time_fn'] = (out_tss_fn-smallest_frame)/10.0
    df['core_departure_time'] = ((df['core_departure_time_fn']-df['radio_arrival_time_fn'])*10)+df['core_departure_time_os']


    # Plot histogram
    ax = axs[axnum]
    ax.set_yscale('log')
    ax.set_xlim([0,9])
    ax.hist(out_tss_ms, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Core Departure Times")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Offset [ms]")
    axnum = axnum+1

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'time1.png')



    #####################################################################################

    fig, ax = plt.subplots(1,1,figsize=(6/1.5,2/1.5))
    N=3
    ax.set_yscale('log')
    ax.set_xlim([0,(10*N)-1])
    ax.set_ylim([1e-4,3])
    plt.grid()
    ax.hist(df['radio_arrival_time_os'], bins=500, density=True, alpha=0.75, color='#003366',label='Radio arrival')
    ax.hist(df['service_time'], bins=500, density=True, alpha=0.75, color='#8B0000',label='Service')
    ax.hist(df['radio_departure_time'], bins=500, density=True, alpha=0.75, color='#333333',label='Radio departure')
    ax.hist(
        df['core_departure_time'],#-33.0, 
        bins=500, density=True, alpha=0.75, color='#004c00',label='Core departure'
    )

    # Draw vertical lines at x=2 and x=4
    ax.text(0.5, 0.5, 'Frame 1', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=9, color='black', linestyle='-')
    ax.text(9.5, 0.5, 'Frame 2', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=19, color='black', linestyle='-')
    ax.text(19.5, 0.5, 'Frame 3', color='black', fontsize=7, ha='left', va='bottom')
    #ax.axvline(x=27, color='black', linestyle='-')
    #ax.axvline(x=36, color='black', linestyle='-')


    # Adjust layout
    plt.tight_layout()
    ax.legend(loc='center right',fontsize=7)

    # Set new x-axis ticks and labels
    ax.set_xticks(list(range(0,10*N,2)))
    ax.set_xticklabels([str(num) for num in list(range(0,10*N,2))], rotation=0)

    ax.set_ylabel("Probability")
    ax.set_xlabel("Subframe")

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'time2.png')

    #####################################################################################

    fig, ax = plt.subplots(1,1,figsize=(6/1.5,2/1.5))
    N=3
    ax.set_yscale('log')
    ax.set_xlim([0,(10*N)-1])
    ax.set_ylim([1e-4,3])
    plt.grid()

    # Filter core departure times
    mask = (df['radio_departure_time'] >= 7.0) & (df['radio_departure_time'] <= 9.0)
    filtered_df = df[mask]

    ax.hist(filtered_df['radio_arrival_time_os'], bins=500, density=True, alpha=0.75, color='#003366',label='Radio arrival')
    ax.hist(filtered_df['service_time'], bins=500, density=True, alpha=0.75, color='#8B0000',label='Service')
    ax.hist(filtered_df['radio_departure_time'], bins=500, density=True, alpha=0.75, color='#333333',label='Radio departure')
    ax.hist(
        filtered_df['core_departure_time'],#-33.0, 
        bins=500, density=True, alpha=0.75, color='#004c00',label='Core departure'
    )

    # Draw vertical lines at x=2 and x=4
    ax.text(0.5, 0.5, 'Frame 1', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=9, color='black', linestyle='-')
    ax.text(9.5, 0.5, 'Frame 2', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=19, color='black', linestyle='-')
    ax.text(19.5, 0.5, 'Frame 3', color='black', fontsize=7, ha='left', va='bottom')
    #ax.axvline(x=27, color='black', linestyle='-')
    #ax.axvline(x=36, color='black', linestyle='-')


    # Adjust layout
    plt.tight_layout()
    ax.legend(loc='center right',fontsize=7)

    # Set new x-axis ticks and labels
    ax.set_xticks(list(range(0,10*N,2)))
    ax.set_xticklabels([str(num) for num in list(range(0,10*N,2))], rotation=0)

    ax.set_ylabel("Probability")
    ax.set_xlabel("Subframe")

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'time3.png')

#####################################################################################

    fig, ax = plt.subplots(1,1,figsize=(6/1.5,2/1.5))
    N=3
    ax.set_yscale('log')
    ax.set_xlim([0,(10*N)-1])
    ax.set_ylim([1e-4,3])
    plt.grid()

    # Filter core departure times
    mask = (df['radio_departure_time'] >= 12.0) & (df['radio_departure_time'] <= 14.0)
    filtered_df = df[mask]

    ax.hist(filtered_df['radio_arrival_time_os'], bins=500, density=True, alpha=0.75, color='#003366',label='Radio arrival')
    ax.hist(filtered_df['service_time'], bins=500, density=True, alpha=0.75, color='#8B0000',label='Service')
    ax.hist(filtered_df['radio_departure_time'], bins=500, density=True, alpha=0.75, color='#333333',label='Radio departure')
    ax.hist(
        filtered_df['core_departure_time'],#-33.0, 
        bins=500, density=True, alpha=0.75, color='#004c00',label='Core departure'
    )

    # Draw vertical lines at x=2 and x=4
    ax.text(0.5, 0.5, 'Frame 1', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=9, color='black', linestyle='-')
    ax.text(9.5, 0.5, 'Frame 2', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=19, color='black', linestyle='-')
    ax.text(19.5, 0.5, 'Frame 3', color='black', fontsize=7, ha='left', va='bottom')
    #ax.axvline(x=27, color='black', linestyle='-')
    #ax.axvline(x=36, color='black', linestyle='-')


    # Adjust layout
    plt.tight_layout()
    ax.legend(loc='center right',fontsize=7)

    # Set new x-axis ticks and labels
    ax.set_xticks(list(range(0,10*N,2)))
    ax.set_xticklabels([str(num) for num in list(range(0,10*N,2))], rotation=0)

    ax.set_ylabel("Probability")
    ax.set_xlabel("Subframe")

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'time4.png')

    #####################################################################################

    fig, ax = plt.subplots(1,1,figsize=(6/1.5,2/1.5))
    N=3
    ax.set_yscale('log')
    ax.set_xlim([0,(10*N)-1])
    ax.set_ylim([1e-4,3])
    plt.grid()

    # Filter core departure times
    mask = (df['radio_departure_time'] >= 17.0) & (df['radio_departure_time'] <= 19.0)
    filtered_df = df[mask]

    ax.hist(filtered_df['radio_arrival_time_os'], bins=500, density=True, alpha=0.75, color='#003366',label='Radio arrival')
    ax.hist(filtered_df['service_time'], bins=500, density=True, alpha=0.75, color='#8B0000',label='Service')
    ax.hist(filtered_df['radio_departure_time'], bins=500, density=True, alpha=0.75, color='#333333',label='Radio departure')
    ax.hist(
        filtered_df['core_departure_time'],#-33.0, 
        bins=500, density=True, alpha=0.75, color='#004c00',label='Core departure'
    )

    # Draw vertical lines at x=2 and x=4
    ax.text(0.5, 0.5, 'Frame 1', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=9, color='black', linestyle='-')
    ax.text(9.5, 0.5, 'Frame 2', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=19, color='black', linestyle='-')
    ax.text(19.5, 0.5, 'Frame 3', color='black', fontsize=7, ha='left', va='bottom')
    #ax.axvline(x=27, color='black', linestyle='-')
    #ax.axvline(x=36, color='black', linestyle='-')


    # Adjust layout
    plt.tight_layout()
    ax.legend(loc='center right',fontsize=7)

    # Set new x-axis ticks and labels
    ax.set_xticks(list(range(0,10*N,2)))
    ax.set_xticklabels([str(num) for num in list(range(0,10*N,2))], rotation=0)

    ax.set_ylabel("Probability")
    ax.set_xlabel("Subframe")

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'time5.png')

    #####################################################################################


    fig, ax = plt.subplots(1,1,figsize=(6/1.5,2/1.5))
    N=3
    ax.set_yscale('log')
    ax.set_xlim([0,(10*N)-1])
    ax.set_ylim([1e-4,3])
    plt.grid()

    # Filter core departure times
    mask = (df['radio_departure_time'] >= 27.0) & (df['radio_departure_time'] <= 29.0)
    filtered_df = df[mask]

    ax.hist(filtered_df['radio_arrival_time_os'], bins=500, density=True, alpha=0.75, color='#003366',label='Radio arrival')
    ax.hist(filtered_df['service_time'], bins=500, density=True, alpha=0.75, color='#8B0000',label='Service')
    ax.hist(filtered_df['radio_departure_time'], bins=500, density=True, alpha=0.75, color='#333333',label='Radio departure')
    ax.hist(
        filtered_df['core_departure_time'],#-33.0, 
        bins=500, density=True, alpha=0.75, color='#004c00',label='Core departure'
    )

    # Draw vertical lines at x=2 and x=4
    ax.text(0.5, 0.5, 'Frame 1', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=9, color='black', linestyle='-')
    ax.text(9.5, 0.5, 'Frame 2', color='black', fontsize=7, ha='left', va='bottom')
    ax.axvline(x=19, color='black', linestyle='-')
    ax.text(19.5, 0.5, 'Frame 3', color='black', fontsize=7, ha='left', va='bottom')
    #ax.axvline(x=27, color='black', linestyle='-')
    #ax.axvline(x=36, color='black', linestyle='-')


    # Adjust layout
    plt.tight_layout()
    ax.legend(loc='center right',fontsize=7)

    # Set new x-axis ticks and labels
    ax.set_xticks(list(range(0,10*N,2)))
    ax.set_xticklabels([str(num) for num in list(range(0,10*N,2))], rotation=0)

    ax.set_ylabel("Probability")
    ax.set_xlabel("Subframe")

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'time6.png')