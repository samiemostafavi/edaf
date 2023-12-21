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
    ax.set_ylim(1e-4, 1)
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
    ax.set_ylim(1e-4, 1)
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
    ax.set_ylim(1e-4, 1)
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
    timestamp1 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.0.timestamp"]
    timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000
    df['link_delay'] = timestamp_difference
    df['link_delay_perc'] = timestamp_difference / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 25)  # Set x limits
    ax.set_ylim(1e-4, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Link Delay")
    #ax.set_ylabel("Probability")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    axnum = axnum+1

    ################### Segmentation delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.last.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000
    df['segmentation_delay'] = timestamp_difference
    df['segmentation_delay_perc'] = timestamp_difference / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 5)  # Set x limits
    ax.set_ylim(1e-4, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(tx_timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Segmentation Delay")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1

    ################### Transmission delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.first.timestamp"]
    timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000
    df['transmission_delay'] = timestamp_difference
    df['transmission_delay_perc'] = timestamp_difference / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 5)  # Set x limits
    ax.set_ylim(1e-4, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(tx_timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax.set_title("Transmission Delay")
    ax.set_ylabel("CCDF")
    ax.set_xlabel("Delay [ms]")
    #ax.set_yscale('log')  # Set y-axis to log scale
    axnum = axnum+1

    ################### Retransmissions delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.reassembled.0.mac.demuxed.timestamp"]
    timestamp2 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.first.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000
    df['retransmission_delay'] = timestamp_difference
    df['retransmission_delay_perc'] = timestamp_difference / df['e2e_delay']

    # Plot histogram
    ax = axs[axnum]
    #ax.set_xlim(0, 30)  # Set x limits
    ax.set_ylim(1e-4, 1)
    sns.ecdfplot(timestamp_difference, complementary=True, log_scale=(False, True), ax=ax, color='Blue')
    #ax.hist(timestamp_difference, bins=100, density=True, cumulative=-1, log=True, alpha=0.75, color='blue')
    ax.set_title("Retransmissions Delay")
    #ax.set_ylabel("Probability")
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
    ax.set_xlim([0,y_points[largest_index]])

    ax.set_xlabel("Delay [ms]")
    ax.set_ylabel("Portion")
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

    exit(0)

    ###########################################################################################

    fig, ax = plt.subplots(1,1,figsize=(3,2))
    ax.set_yscale('log')
    ax.set_ylim(1e-4, 1)

    markevery = 0.05
    markersize = 4
    linewidth = 1.5


    y_points_lim = [0,30,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'e2e_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color='#004c00',
        linewidth=linewidth,
        linestyle='-',
        #markersize=markersize,
        #markevery=markevery,
        #marker='o',
        #mfc='none',
        label="End-to-end delay"
    )

    y_points_lim = [0,30,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'link_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color= '#003366',
        linewidth=linewidth,
        linestyle=':',
        #markersize=markersize,
        #markevery=markevery,
        #marker='s',
        #mfc='none',
        label="Link delay",
    )

    y_points_lim = [0,3,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'core_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color='#8B0000',
        linewidth=linewidth,
        linestyle='--',
        #markersize=markersize,
        #markevery=markevery,
        #marker='v',
        #mfc='none',
        label="Core delay"
    )

    y_points_lim = [0,10,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'queuing_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color='#333333',
        linewidth=linewidth,
        linestyle='-.',
        #markersize=markersize,
        #markevery=markevery,
        #marker='x',
        #mfc='none',
        label="Queuing delay"
    )

    ax.set_xlabel("Delay [ms]")
    ax.set_ylabel("CCDF")
    ax.grid(visible=True)
    ax.legend()

    plt.legend(loc='upper right')

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'decompose2.png')


    ########################################################################################################

    fig, ax = plt.subplots(1,1,figsize=(3,2))
    ax.set_yscale('log')
    ax.set_ylim(1e-4, 1)

    markevery = 0.05
    markersize = 4
    linewidth = 1.5

    y_points_lim = [0,30,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'link_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color='#333333',
        linewidth=linewidth,
        linestyle='-.',
        #markersize=markersize,
        #markevery=markevery,
        #marker='x',
        #mfc='none',
        label="Link delay"
    )


    y_points_lim = [0,10,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'segmentation_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color='#004c00',
        linewidth=linewidth,
        linestyle='-',
        #markersize=markersize,
        #markevery=markevery,
        #marker='o',
        #mfc='none',
        label="Segmentation delay"
    )

    y_points_lim = [0,10,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'transmission_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color= '#003366',
        linewidth=linewidth,
        linestyle=':',
        #markersize=markersize,
        #markevery=markevery,
        #marker='s',
        #mfc='none',
        label="Transmission delay",
    )

    y_points_lim = [0,30,100]
    y_points = np.linspace(
        start=y_points_lim[0],
        stop=y_points_lim[1],
        num=y_points_lim[2],
    )
    emp_cdf, emp_pdf = calc_emp_prob(df,'retransmission_delay',y_points,y_points_lim)
    ax.plot(
        y_points,
        np.float64(1.00)-np.array(emp_cdf,dtype=np.float64),
        color='#8B0000',
        linewidth=linewidth,
        linestyle='--',
        #markersize=markersize,
        #markevery=markevery,
        #marker='v',
        #mfc='none',
        label="Retransmission delay"
    )

    ax.set_xlabel("Delay [ms]")
    ax.set_ylabel("CCDF")
    ax.grid(visible=True)
    ax.legend()

    plt.legend(loc='upper right')

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'decompose3.png')


    ########################################################################################################
    # List of 'y' values
    y_values = np.linspace(5,15,100)

    fig, ax = plt.subplots(1,1,figsize=(3,2))

    percentage_column_names = ["retransmission_delay_perc", "transmission_delay_perc", 
                            "segmentation_delay_perc", "queuing_delay_perc", "core_delay_perc"]

    # Create a new DataFrame to store the normalized values
    normalized_df = pd.DataFrame(index=y_values, columns=percentage_column_names)

    #dark_palette = ["#004c00", "#8B0000", "#003366", "#333333", "#800080"]

    # Calculate the normalized values for each 'y' value
    for y in y_values:
        df_filtered = df[df['e2e_delay'] > y]
        total_values = df_filtered[percentage_column_names].sum(axis=1)
        
        # Normalize the values so that the sum is equal to one
        normalized_values = df_filtered[percentage_column_names].div(total_values, axis=0)
        
        normalized_df.loc[y] = normalized_values.mean()

    # Plot the results
    ax = normalized_df.plot(kind='bar', colormap='viridis', alpha=0.8, stacked=True, width=1,ax=ax)
    
    # add hatch
    #ax.containers[:4]
    #bars = [thing for thing in ax.containers if isinstance(thing,mpl.container.BarContainer)]
    #import itertools
    #patterns = itertools.cycle(('||', 'x', '\\', '..', 'O'))
    #d = {}
    #for bar in bars:
    #    for patch in bar:
    #        pat = d.setdefault(patch.get_facecolor(), next(patterns))
    #        patch.set_hatch(pat)
    #L = ax.legend()

    # Get x-axis ticks and labels
    x_ticks = ax.get_xticks()
    x_labels = [label.get_text() for label in ax.get_xticklabels()]

    req_x_ticks = np.linspace(5,15,11) #list(range(16))
    float_numbers = [float(num_str) for num_str in x_labels]
    new_x_ticks = []
    new_x_labels = []
    for req_x_tick in req_x_ticks:
        differences = [abs(req_x_tick - float_num) for float_num in float_numbers]
        closest_index = differences.index(min(differences))
        new_x_ticks.append(x_ticks[closest_index])
        new_x_labels.append(str(req_x_tick))

    # Set new x-axis ticks and labels
    ax.set_xticks(new_x_ticks)
    ax.set_xticklabels(new_x_labels, rotation=0)

    # Customize plot attributes
    plt.xlabel('Delay target [ms]')
    ax.set_ylim(0,1)
    plt.ylabel('Portion')
    ax.legend(
        labels=['Retransmission delay', 'Transmission delay', 'Segmentation delay', 'Queuing delay', 'Core delay'],facecolor="white", 
        frameon = True,
        loc='upper left'
    )

    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig(Path(sys.argv[2])/'decompose4.png')


###############################################################
