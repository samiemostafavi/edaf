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

if __name__ == "__main__":
    # Check if the file name is provided as a command-line argument
    if len(sys.argv) != 3:
        logger.error("Usage: python script_name.py <parquet_file> <output_figure_file>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    file_path = sys.argv[1]

    # Call the function
    df = read_parquet_file(file_path)

    # Create subplots
    fig, axs = plt.subplots(4, 3, figsize=(10, 10))
    axs = axs.flatten()
    ax1 = axs[0]
    ax2 = axs[1]
    ax3 = axs[2]
    ax4 = axs[3]
    ax5 = axs[4]
    ax6 = axs[5]
    ax7 = axs[6]
    ax8 = axs[7]
    ax9 = axs[8]
    ax10 = axs[9]
    ax11 = axs[10]
    ax12 = axs[11]

    ################### Total RAN delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    ran_timestamp1 = df["gtp.out.timestamp"]
    ran_timestamp2 = df["ip.in.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    ran_timestamp_difference = (ran_timestamp1 - ran_timestamp2) * 1000

    # Plot histogram
    ax1.hist(ran_timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax1.set_title("Total RAN Delay")
    #ax1.set_ylabel("Probability")
    ax1.set_ylabel("CCDF")
    ax1.set_xlabel("Delay [ms]")
    #ax1.set_xlim(0, 20)  # Set x limits
    

    ################### queuing delay + scheduling delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.queue.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000

    # Plot histogram
    ax2.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax2.set_title("Queue + Scheduling delay")
    #ax2.set_ylabel("Probability")
    ax2.set_ylabel("CCDF")
    ax2.set_xlabel("Delay [ms]")
    #ax2.set_xlim(2, 6)  # Set x limits

    ################### Transmission delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    tx_timestamp1 = df["rlc.reassembled.0.mac.demuxed.timestamp"]
    tx_timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    tx_timestamp_difference = (tx_timestamp1 - tx_timestamp2) * 1000

    # Plot histogram
    ax3.hist(tx_timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax3.set_title("TX Delay")
    #ax3.set_ylabel("Probability")
    ax3.set_ylabel("CCDF")
    ax3.set_xlabel("Delay [ms]")
    #ax3.set_xlim(0, 20)  # Set x limits
    #ax3.set_yscale('log')  # Set y-axis to log scale

    # rlc queue
    rlcqueue = df["rlc.queue.queue"]

    # Plot histogram
    ax4.hist(rlcqueue, bins=100, density=True, alpha=0.75, cumulative=-1, log=True, color='blue')
    ax4.set_title("RLC Queue")
    #ax4.set_ylabel("Probability")
    ax4.set_ylabel("CCDF")
    ax4.set_xlabel("Queue length [bytes]")

    ################### RLC segments ###################
    df['non_empty_count'] = df[['rlc.reassembled.0.rlc.reassembled.timestamp',
                            #'rlc.reassembled.1.rlc.reassembled.timestamp',
                            #'rlc.reassembled.2.rlc.reassembled.timestamp',
                            #'rlc.reassembled.3.rlc.reassembled.timestamp',
                        ]].count(axis=1)
    
    # Plot histogram
    ax5.hist(df['non_empty_count'], bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax5.set_title("No. RLC segments")
    #ax5.set_ylabel("Probability")
    ax5.set_ylabel("CCDF")
    ax5.set_xlabel("Number of segments")

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
    ax6.hist(df['hqround_total'], bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax6.set_title("HARQ rounds")
    #ax6.set_ylabel("Probability")
    ax6.set_ylabel("CCDF")
    ax6.set_xlabel("Number of rounds")
    #ax6.set_yscale('log')  # Set y-axis to log scale


    ################### Find 5G TDD Time Offset ###################
    frame_length = 10 # ms
    ul_slots = [7,8,9,17,18,19]
    slot_dur = 0.5 # ms
    ul_slots_offsets = [ slot_num*slot_dur for slot_num in ul_slots ]
    ul_slots_tss = [ [] for _ in ul_slots ]
    out_slots = list(df["rlc.queue.segments.0.mac.sdu.slot"])
    out_tss = list(df["rlc.queue.segments.0.mac.sdu.timestamp"])
    for idx, out_ts in enumerate(out_tss):
        if int(out_slots[idx]) in ul_slots:
            idy = ul_slots.index(int(out_slots[idx]))
            out_ts_ms = (out_ts * 1000) - (np.floor(out_ts * 100)*10)
            ul_slots_tss[idy].append(out_ts_ms)

    res = []
    for out_ts_mss in ul_slots_tss:
        if len(out_ts_mss) != 0:
            res.append(np.mean(out_ts_mss))
        else:
            res.append(np.NaN)

    #remove NaNs
    nonan_res = []
    nonan_ul_slots = []
    nonan_ul_slots_offsets = []
    for idx,item in enumerate(res):
        if not np.isnan(item):
            nonan_res.append(item)
            nonan_ul_slots.append(ul_slots[idx])
            nonan_ul_slots_offsets.append(ul_slots_offsets[idx])

    logger.info(f"For uplink slots {nonan_ul_slots}, timestamps received: {nonan_res}")
    tmp = np.array(nonan_res) - np.array(nonan_ul_slots_offsets)
    for idx,item in enumerate(tmp):
        if item < 0:
            tmp[idx] = tmp[idx] + 10
    tdd_clock_offset_ms = np.mean(np.array(tmp))
    logger.info(f"Difference array: {tmp}")
    logger.info(f"Estimated offset between {nonan_ul_slots_offsets} and {nonan_res}: {tdd_clock_offset_ms}ms")


    ################### TX Time Offset ###################
    tx_tss_ms = list( (df['ip.in.timestamp'] * 1000) - (np.floor(df['ip.in.timestamp'] * 100)*10) )
    tx_tss_ms_nooff = []
    for idx, tx_ts_ms in enumerate(tx_tss_ms):
        val = (tx_ts_ms + tdd_clock_offset_ms) % 10
        tx_tss_ms_nooff.append( val )

    # Plot histogram
    ax7.hist(tx_tss_ms_nooff, bins=100, density=True, alpha=0.75, color='blue')
    ax7.set_title("UE TX time offset")
    ax7.set_ylabel("Probability")
    ax7.set_xlim([0,9])
    ax7.set_xlabel("Offset [ms]")

    ################### GNB RX Times ###################
    out_tss = list(df["rlc.queue.segments.0.mac.sdu.timestamp"])
    out_tss_ms = []
    for idx, out_ts in enumerate(out_tss):
        val = (((out_ts * 1000) - (np.floor(out_ts * 100)*10)) + tdd_clock_offset_ms) % 10
        out_tss_ms.append( val )

    # Plot histogram
    ax8.hist(out_tss_ms, bins=100, density=True, alpha=0.75, color='blue')
    ax8.set_title("GNB RX time offset")
    ax8.set_ylabel("Probability")
    ax8.set_xlim([0,9])
    ax8.set_xlabel("Offset [ms]")

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(sys.argv[2])


    ################### Filter Late Packets ###################

    filter_slots = [7,8,9]

    # Define a function to filter rows based on the condition
    def filter_rows(row):
        hqround_value = row['rlc.reassembled.0.mac.demuxed.hqround']
        decoded_slot_column = f'rlc.reassembled.0.mac.demuxed.mac.decoded.{hqround_value}.slot'
        return row[decoded_slot_column] in filter_slots

    # Apply the function to filter rows
    filtered_df = df[df.apply(filter_rows, axis=1)]

    ################### Filtered GNB RX Times ###################

    out_tss = list(filtered_df["rlc.queue.segments.0.mac.sdu.timestamp"])
    out_tss_ms = []
    for idx, out_ts in enumerate(out_tss):
        val = (((out_ts * 1000) - (np.floor(out_ts * 100)*10)) + tdd_clock_offset_ms) % 10
        out_tss_ms.append( val )

    # Plot histogram
    ax9.hist(out_tss_ms, bins=100, density=True, alpha=0.75, color='blue')
    ax9.set_title("New GNB RX time offset")
    ax9.set_ylabel("Probability")
    ax9.set_xlim([0,9])
    ax9.set_xlabel("Offset [ms]")


    ################### Filtered Total RAN delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    ran_timestamp1 = filtered_df["gtp.out.timestamp"]
    ran_timestamp2 = filtered_df["ip.in.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    ran_timestamp_difference = (ran_timestamp1 - ran_timestamp2) * 1000

    # Plot histogram
    ax10.hist(ran_timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax10.set_title("New Total RAN Delay")
    #ax10.set_ylabel("Probability")
    ax10.set_ylabel("CCDF")
    ax10.set_xlabel("Delay [ms]")
    #ax10.set_xlim(0, 20)  # Set x limits


    ################### Filtered queuing delay + scheduling delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = filtered_df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = filtered_df["rlc.queue.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000

    # Plot histogram
    ax11.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax11.set_title("New Queue + Scheduling delay")
    #ax2.set_ylabel("Probability")
    ax11.set_ylabel("CCDF")
    ax11.set_xlabel("Delay [ms]")
    #ax2.set_xlim(2, 6)  # Set x limits


    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(sys.argv[2])