import sys
import pandas as pd
from loguru import logger
import numpy as np
import matplotlib.pyplot as plt

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

        self.ul_slots_ref_ts = [ slot_num*self.slot_dur for slot_num in self.ul_slots ]
        logger.info(f"Reference timestamps: {self.ul_slots_ref_ts} in ms")

        ul_slots_tss = [ [] for _ in self.ul_slots ]
        out_slots = list(df["rlc.queue.segments.0.mac.sdu.slot"])
        out_tss = list(df["rlc.queue.segments.0.mac.sdu.timestamp"])
        for idx, out_ts in enumerate(out_tss):
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
        tmp = np.array(nonan_res) - np.array(nonan_ul_slots_offsets)
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

    # Add a new row: first_hqround_rx_ts
    def get_first_hqround_rx_ts(row):
        hqround_value = row['rlc.reassembled.0.mac.demuxed.hqround']
        decoded_slot_column = f'rlc.reassembled.0.mac.demuxed.mac.decoded.{hqround_value}.timestamp'
        return row[decoded_slot_column]
    # Apply the function to filter rows
    df['rlc.reassembled.0.mac.demuxed.mac.decoded.first_hqround_rx_ts'] = df.apply(get_first_hqround_rx_ts, axis=1)

    tdd_schedule = TDDSchedule(df)

    # Create subplots
    fig, axs = plt.subplots(4, 4, figsize=(15, 10))
    axs = axs.flatten()
    axnum = 0

    ################### Total RAN Latency ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    ran_timestamp1 = df["gtp.out.timestamp"]
    ran_timestamp2 = df["ip.in.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    ran_timestamp_difference = (ran_timestamp1 - ran_timestamp2) * 1000

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim(0, 30)  # Set x limits
    ax.hist(ran_timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Total RAN Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    axnum = axnum+1

    ################### Queuing Delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.queue.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim(0, 15)  # Set x limits
    ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Queuing Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    axnum = axnum+1

    ################### Link Delay ###################
    # "rlc.reassembled.0.mac.demuxed.mac.decoded.timestamp" - "rlc.queue.segments.0.rlc.txpdu.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.0.timestamp"]
    timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim(0, 25)  # Set x limits
    ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Link Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    axnum = axnum+1

    ################### Transmission delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    tx_timestamp1 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.first_hqround_rx_ts"]
    tx_timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    tx_timestamp_difference = (tx_timestamp1 - tx_timestamp2) * 1000

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim(0, 5)  # Set x limits
    ax.hist(tx_timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Transmission Delay")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Delay [ms]")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1

    ################### Retransmissions delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    tx_timestamp1 = df["rlc.reassembled.0.mac.demuxed.timestamp"]
    tx_timestamp2 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.first_hqround_rx_ts"]

    # Convert timestamps to milliseconds and calculate the difference
    tx_timestamp_difference = (tx_timestamp1 - tx_timestamp2) * 1000

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim(0, 30)  # Set x limits
    ax.hist(tx_timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Retransmissions Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1

    ################### hqrounds (total) ###################
    # Add up the specified columns
    #df['hqround_total'] = (
    #    df['rlc.reassembled.0.mac.demuxed.hqround'] #+
    #    #df['rlc.reassembled.1.mac.demuxed.hqround'] +
    #    #df['rlc.reassembled.2.mac.demuxed.hqround'] +
    #    #df['rlc.reassembled.3.mac.demuxed.hqround']
    #)
    df['hqround_total'] = (
        df['rlc.reassembled.0.mac.demuxed.hqround']
    )
    
    # Plot histogram
    ax = axs[axnum]
    ax.hist(df['hqround_total'], bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("HARQ rounds")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Number of rounds")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1

    ################### RLC Queue ###################
    # rlc queue
    rlcqueue = df["rlc.queue.queue"]

    # Plot histogram
    ax = axs[axnum]
    ax.hist(rlcqueue, bins=100, density=True, alpha=0.75, cumulative=-1, log=True, color='blue')
    ax.set_title("RLC Queue")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Queue length [bytes]")
    axnum = axnum+1

    ################### RLC Segments ###################
    # find the maximum number of segments
    num_segments = 0
    rlc_reassembled_strs = []
    for num in range(20):
        if f"rlc.reassembled.{num}.rlc.reassembled.timestamp" not in df.columns:
            num_segments = num - 1
        else:
            rlc_reassembled_strs.append(f"rlc.reassembled.{num}.rlc.reassembled.timestamp")

    df['non_empty_count'] = df[rlc_reassembled_strs].count(axis=1)
    
    # Plot histogram
    ax = axs[axnum]
    ax.hist(df['non_empty_count'], bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Number of segments")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Number of segments")
    axnum = axnum+1

    ################### Arrival Times ###################
    tx_tss_ms = list( (df['ip.in.timestamp'] * 1000) - (np.floor(df['ip.in.timestamp'] * 100)*10) )
    tx_tss_ms_nooff = []
    for idx, tx_ts_ms in enumerate(tx_tss_ms):
        val = (tx_ts_ms + tdd_schedule.tdd_ts_offset_ms) % 10
        tx_tss_ms_nooff.append( val )

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim([0,9])
    ax.hist(tx_tss_ms_nooff, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Arrival Times")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Offset [ms]")
    axnum = axnum+1

    ################### Service Times ###################
    tx_tss_ms = list( (df['rlc.queue.segments.0.rlc.txpdu.timestamp'] * 1000) - (np.floor(df['rlc.queue.segments.0.rlc.txpdu.timestamp'] * 100)*10) )
    tx_tss_ms_nooff = []
    for idx, tx_ts_ms in enumerate(tx_tss_ms):
        val = (tx_ts_ms + tdd_schedule.tdd_ts_offset_ms) % 10
        tx_tss_ms_nooff.append( val )

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim([0,9])
    ax.hist(tx_tss_ms_nooff, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Service Times")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Offset [ms]")
    axnum = axnum+1

    ################### GNB RX Times ###################
    #out_tss = list(df["rlc.queue.segments.0.mac.sdu.timestamp"])
    out_tss = list(df["rlc.reassembled.0.mac.demuxed.mac.decoded.0.timestamp"])
    out_tss_ms = []
    for idx, out_ts in enumerate(out_tss):
        val = (((out_ts * 1000) - (np.floor(out_ts * 100)*10)) + tdd_schedule.tdd_ts_offset_ms) % 10
        out_tss_ms.append( val )

    # Plot histogram
    ax = axs[axnum]
    ax.set_xlim([0,9])
    ax.hist(out_tss_ms, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Departure Times")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Offset [ms]")
    axnum = axnum+1

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(sys.argv[2])
