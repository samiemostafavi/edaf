import sys
import pandas as pd
from loguru import logger
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import matplotlib as mpl
import seaborn as sns

import scienceplots
plt.style.use(['science','ieee'])

MAX_SEGMENTS = 20

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

def calc_emp_prob(df,key_label,y_points,y_points_lim):
        # Calculate emp CDF
        emp_cdf = list()
        for y in y_points:
            emp_cdf.append(
                len(
                    df[df[key_label] <= y]
                ) / len(df)
            )

        # Calculate emp PDF
        emp_pdf = np.diff(np.array(emp_cdf))
        emp_pdf = np.append(emp_pdf,[0])*y_points_lim[1]/(y_points_lim[2]-y_points_lim[0])
        return emp_cdf,emp_pdf


if __name__ == "__main__":
    # Check if the file name is provided as a command-line argument
    if len(sys.argv) != 3:
        logger.error("Usage: python script_name.py <parquet_file> <output_figure_file>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    file_path = sys.argv[1]

    # Call the function
    df = read_parquet_file(file_path)

    # Remove the first and last N elements to filter nonsense packets
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
    def get_first_hqround_rx_ts(row,seg):
        if seg != -1:
            hqround_value = row[f'rlc.reassembled.{seg}.mac.demuxed.hqround']
            if hqround_value >= 0:
                decoded_slot_column = f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.{int(hqround_value)}.timestamp'
                return row[decoded_slot_column]
            else:
                return np.nan
        else:
            hqround_value = row[f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.hqround"]
            if hqround_value >= 0:
                decoded_slot_column = f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.mac.decoded.{int(hqround_value)}.timestamp"
                return row[decoded_slot_column]
            else:
                return np.nan
    for seg in range(MAX_SEGMENTS):
        if f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.0.timestamp' in df:
            df[f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.first.timestamp'] = df.apply(get_first_hqround_rx_ts, args=(seg,), axis=1)
    df['rlc.reassembled.first.mac.demuxed.mac.decoded.first.timestamp'] = df.apply(get_first_hqround_rx_ts, args=(-1,), axis=1)

    # Add a new column: rlc.reassembled.first
    def get_first_segment_reassembly_ts(row):
        first_seg_column = f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.mac.decoded.0.timestamp"
        return row[first_seg_column]
    df["rlc.reassembled.first.mac.demuxed.mac.decoded.0.timestamp"] = df.apply(get_first_segment_reassembly_ts, axis=1)

    # Add a new column: last_segment_txpdu_ts
    def get_last_segment_txpdu_ts(row):
        last_seg = 0
        for i in range(MAX_SEGMENTS):
            if f'rlc.queue.segments.{i}.rlc.txpdu.timestamp' in row:
                if row[f'rlc.queue.segments.{i}.rlc.txpdu.timestamp'] > 0:
                    last_seg = i
        last_seg_column = f'rlc.queue.segments.{last_seg}.rlc.txpdu.timestamp'
        return row[last_seg_column]
    df['rlc.queue.segments.last.rlc.txpdu.timestamp'] = df.apply(get_last_segment_txpdu_ts, axis=1)

    # Create plots
    fig, ax = plt.subplots(1,1, figsize=(10,4))

    # Create subplots
    n_rows = 3
    n_cols = 3
    fig, axs = plt.subplots(n_rows, n_cols, figsize=(5.0*n_cols/3.0, 2.75*n_rows/2.0))
    axs = axs.flatten()
    axnum = 0

    ################### End to End Delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["receive.timestamp"]
    timestamp2 = df["send.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = ((timestamp1 - timestamp2) * 1000) #-32.0
    df['e2e_delay'] = timestamp_difference

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 80)  # Set x limits
    ax.set_ylim(1e-5, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(ran_timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("End-to-End Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")

    axnum = axnum+1

    ################### Core Delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["receive.timestamp"]
    timestamp2 = df["gtp.out.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = ((timestamp1 - timestamp2) * 1000)#-32.0
    df['core_delay'] = timestamp_difference
    df['core_delay_perc'] = timestamp_difference / df['e2e_delay']
    #print(timestamp_difference)

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 80)  # Set x limits
    ax.set_ylim(1e-5, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Core Delay")
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
    df['queuing_delay'] = timestamp_difference
    df['queuing_delay_perc'] = timestamp_difference / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 15)  # Set x limits
    ax.set_ylim(1e-5, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Queuing Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    axnum = axnum+1

    ################### Link Delay ###################
    # "rlc.reassembled.0.mac.demuxed.mac.decoded.timestamp" - "rlc.queue.segments.0.rlc.txpdu.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.0.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp2 - timestamp1) * 1000
    df['link_delay'] = timestamp_difference
    df['link_delay_perc'] = timestamp_difference / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 25)  # Set x limits
    ax.set_ylim(1e-5, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Link Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    axnum = axnum+1

    ################### Transmission delay ###################

    # Add a new column: longest_segment
    def get_max_delay_segment_gnbind(row):
        segment_delays = []
        for i in range(row['rlc.reassembled.num_segments']+1):
            ue_ind = row['rlc.reassembled.num_segments']-i
            gnb_ind = i
            timestamp1 = row[f"rlc.queue.segments.{ue_ind}.rlc.txpdu.timestamp"]
            timestamp2 = row[f"rlc.reassembled.{gnb_ind}.mac.demuxed.mac.decoded.0.timestamp"]
            segment_delays.append((timestamp2-timestamp1)*1000)
        max_index = segment_delays.index(max(segment_delays))
        return max_index
    df['max_delay_segment_gnbind'] = df.apply(get_max_delay_segment_gnbind, axis=1)

    # Add a new column: get longest_segment_tx_delay
    def get_longest_segment_tx_delay(row):
        ue_ind = row['rlc.reassembled.num_segments']-row['max_delay_segment_gnbind']
        gnb_ind = row['max_delay_segment_gnbind']
        timestamp1 = row[f"rlc.queue.segments.{ue_ind}.rlc.txpdu.timestamp"]
        timestamp2 = row[f"rlc.reassembled.{gnb_ind}.mac.demuxed.mac.decoded.first.timestamp"]
        return (timestamp2-timestamp1)*1000
    df['longest_segment_tx_delay_ms'] = df.apply(get_longest_segment_tx_delay, axis=1)

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = df['longest_segment_tx_delay_ms']
    df['transmission_delay'] = timestamp_difference
    # Remove rows where 'transmission_delay' is less than 0
    df = df[df['transmission_delay'] >= 0]
    df['transmission_delay_perc'] = timestamp_difference / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 5)  # Set x limits
    ax.set_ylim(1e-5, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(tx_timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Transmission Delay")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1

    ################### Retransmissions delay ###################

    def get_longest_segment_retx_delay(row):
        ind = row['max_delay_segment_gnbind']
        timestamp1 = row[f"rlc.reassembled.{ind}.mac.demuxed.mac.decoded.first.timestamp"]
        timestamp2 = row[f"rlc.reassembled.{ind}.mac.demuxed.mac.decoded.0.timestamp"]
        return (timestamp2-timestamp1)*1000
    df['longest_segment_retx_delay_ms'] = df.apply(get_longest_segment_retx_delay, axis=1)

    # Add a new column: retransmission_delay
    def calc_full_retransmission_delay(row):
        sum_delay = 0.0
        for seg in range(row['rlc.reassembled.num_segments']):
            timestamp1 = row[f"rlc.reassembled.{seg}.mac.demuxed.mac.decoded.first.timestamp"]
            timestamp2 = row[f"rlc.reassembled.{seg}.mac.demuxed.mac.decoded.0.timestamp"]
            sum_delay += ((timestamp2 - timestamp1) * 1000)
        return sum_delay
    # Apply the function to filter rows
    df["full_retransmission_delay"] = df.apply(calc_full_retransmission_delay, axis=1)

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = df['longest_segment_retx_delay_ms']
    df["retransmission_delay"] = timestamp_difference
    df['retransmission_delay_perc'] = df['retransmission_delay'] / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 30)  # Set x limits
    ax.set_ylim(1e-5, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Retransmissions Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1


    ################### Segmentation delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    # timestamp1 = df["rlc.queue.segments.last.rlc.txpdu.timestamp"]
    # timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp1 = df["rlc.reassembled.first.mac.demuxed.mac.decoded.first.timestamp"]
    timestamp2 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.first.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = df['link_delay']-(df['longest_segment_retx_delay_ms']+df['longest_segment_tx_delay_ms'])
    df['segmentation_delay'] = timestamp_difference
    # Remove rows where 'transmission_delay' is less than 0
    df = df[df['segmentation_delay'] >= 0]
    df['segmentation_delay_perc'] = df['segmentation_delay'] / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 5)  # Set x limits
    ax.set_ylim(1e-5, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(tx_timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Segmentation Delay")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'decompose1.png')

    ###########################################################################################

    fig, ax = plt.subplots(1,1,figsize=(3,2))
    #ax.set_yscale('log')
    ax.set_ylim(1e-4, 1)
    #ax.set_xlim([0, 23])

    markevery = 0.05
    markersize = 4
    linewidth = 1.5

    y_points_lim = [0,30,300]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'e2e_delay',y_points,y_points_lim)

    emp_tail = np.float64(1.00)-np.array(emp_cdf,dtype=np.float64)
    # Stacked area plot
    #ax.fill_between(y_points, 0, emp_tail, label='X', alpha=0.5)
    #ax.fill_between(y_points, 0, ccdf_X2, label='X2', alpha=0.5)

    percentage_column_names = ["retransmission_delay_perc", "transmission_delay_perc", 
                            "segmentation_delay_perc", "queuing_delay_perc", "core_delay_perc"]
    # Create a new DataFrame to store the normalized values
    normalized_df = pd.DataFrame(index=y_points, columns=percentage_column_names)
    # Calculate the normalized values for each 'y' value
    for y in y_points:
        df_filtered = df[df['e2e_delay'] > y]
        total_values = df_filtered[percentage_column_names].sum(axis=1)
        # Normalize the values so that the sum is equal to one
        normalized_values = df_filtered[percentage_column_names].div(total_values, axis=0)
        normalized_df.loc[y] = normalized_values.mean()
    
    Lt = np.array(normalized_df['transmission_delay_perc'].to_list())
    Ls = np.array(normalized_df['segmentation_delay_perc'].to_list())
    dQ = np.array(normalized_df['queuing_delay_perc'].to_list())
    Lr = np.array(normalized_df['retransmission_delay_perc'].to_list())
    dC = np.array(normalized_df['core_delay_perc'].to_list())

    #colors = plt.cm.viridis(np.linspace(0, 1, 6))
    #colors = plt.cm.Pastel1(np.linspace(0, 1, 6))
    #colors = plt.cm.Set2(np.random.uniform(6))
    #colors = plt.cm.Set2([1,2,6,0])
    #colors = plt.cm.tab20b((np.array([0,1,2,3,4])*4)+2)
    #alpha=0.5
    colors = plt.cm.tab20c((np.array([0,1,2,3,4])*4)+2)
    alpha=0.8
    #colors = plt.cm.GnBu(np.linspace(0, 1, 6))
    #colors = plt.cm.tab10(np.linspace(0, 1, 6))
    ax.fill_between(y_points, 0, Lr, label='Retrans.', color=colors[0], alpha=alpha)
    ax.fill_between(y_points, Lr, Lr+Lt, label='Trans.', color=colors[1], alpha=alpha)
    ax.fill_between(y_points, Lr+Lt, Lr+Lt+Ls, label='Segment.', color=colors[2], alpha=alpha)
    ax.fill_between(y_points, Lr+Lt+Ls, Lr+Lt+Ls+dQ, label='Queuing', color=colors[3], alpha=alpha)
    ax.fill_between(y_points, Lr+Lt+Ls+dQ, Lr+Lt+Ls+dQ+dC, label='Core', color=colors[4], alpha=alpha)

    finite_indices = np.where(np.isfinite(dC))[0]
    largest_index = np.max(finite_indices)
    #ax.set_xlim([0,y_points[largest_index]])
    ax.set_xlim([0,14])

    ax.set_xlabel("Delay [ms]")
    ax.set_ylabel("Percentage")
    ax.legend(
        loc='lower left',
        labelcolor='black'
    )

    #ax.grid(visible=True)

    #ax.tick_params(
    #    axis='x',          # changes apply to the x-axis
    #    which='both',      # both major and minor ticks are affected
    #    bottom=True,      # ticks along the bottom edge are off
    #    top=False,         # ticks along the top edge are off
    #)

    # Create the second plot sharing the same x-axis
    ax2 = ax.twinx()
    ax2.set_ylabel("CCDF")
    ax2.set_yscale('log')
    ax2.set_xlim([0, 23])

    emp_cdf, emp_pdf = calc_emp_prob(df,'e2e_delay',y_points,y_points_lim)
    ax2.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color='black',
        alpha=0.7,
        linewidth=linewidth,
        linestyle='-',
        #markersize=markersize,
        #markevery=markevery,
        #marker='o',
        #mfc='none',
        label="E2E CCDF"
    )

    ax.set_xlim([0,14.5])

    ax2.set_ylim([1e-5,1])
    
    #ax2.grid(visible=True)
    ax2.legend(
        loc='upper right',
        labelcolor='black'
    )

    ax2.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=True,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
    )


    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'decompose5.png')
