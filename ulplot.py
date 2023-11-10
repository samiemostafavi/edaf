import sys
import pandas as pd
from loguru import logger
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
    if len(sys.argv) != 2:
        logger.error("Usage: python script_name.py <parquet_file>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    file_path = sys.argv[1]

    # Call the function
    df = read_parquet_file(file_path)

    # Create subplots
    fig, axs = plt.subplots(2, 3, figsize=(10, 6))
    axs = axs.flatten()
    ax1 = axs[0]
    ax2 = axs[1]
    ax3 = axs[2]
    ax4 = axs[3]
    ax5 = axs[4]
    ax6 = axs[5]

    # RAN delay
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    ran_timestamp1 = df["gtp.out.timestamp"]
    ran_timestamp2 = df["ip.in.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    ran_timestamp_difference = (ran_timestamp1 - ran_timestamp2) * 1000

    # Plot histogram
    ax1.hist(ran_timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax1.set_title("RAN Delay")
    ax1.set_ylabel("Probability")
    ax1.set_xlabel("Delay [ms]")
    ax1.set_xlim(0, 20)  # Set x limits
    

    # queuing delay + scheduling delay
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.queue.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000

    # Plot histogram
    ax2.hist(timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax2.set_title("Queue + Scheduling delay")
    ax2.set_ylabel("Probability")
    ax2.set_xlabel("Delay [ms]")
    ax2.set_xlim(2, 6)  # Set x limits

    # Transmission delay
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    tx_timestamp1 = df["rlc.reassembled.0.mac.demuxed.timestamp"]
    tx_timestamp2 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    tx_timestamp_difference = (tx_timestamp1 - tx_timestamp2) * 1000

    # Plot histogram
    ax3.hist(tx_timestamp_difference, bins=100, density=True, alpha=0.75, color='blue')
    ax3.set_title("TX Delay")
    ax3.set_ylabel("Probability")
    ax3.set_xlabel("Delay [ms]")
    ax3.set_xlim(0, 20)  # Set x limits

    # rlc queue
    rlcqueue = df["rlc.queue.queue"]

    # Plot histogram
    ax4.hist(rlcqueue, bins=100, density=True, alpha=0.75, color='blue')
    ax4.set_title("RLC Queue")
    ax4.set_ylabel("Probability")
    ax4.set_xlabel("Queue length [bytes]")

    # rlc segments
    df['non_empty_count'] = df[['rlc.reassembled.0.rlc.reassembled.timestamp',
                            'rlc.reassembled.1.rlc.reassembled.timestamp',
                            'rlc.reassembled.2.rlc.reassembled.timestamp',
                            'rlc.reassembled.3.rlc.reassembled.timestamp']].count(axis=1)
    
    # Plot histogram
    ax5.hist(df['non_empty_count'], bins=100, density=True, alpha=0.75, color='blue')
    ax5.set_title("RLC segmentations")
    ax5.set_ylabel("Probability")
    ax5.set_xlabel("Number of segments")


    # hqrounds (total)
    # Add up the specified columns
    df['hqround_total'] = (
        df['rlc.reassembled.0.mac.demuxed.hqround'] +
        df['rlc.reassembled.1.mac.demuxed.hqround'] +
        df['rlc.reassembled.2.mac.demuxed.hqround'] +
        df['rlc.reassembled.3.mac.demuxed.hqround']
    )
    
    # Plot histogram
    ax6.hist(df['hqround_total'], bins=100, density=True, alpha=0.75, color='blue')
    #ax4.set_title("RLC segmentations")
    #ax4.set_ylabel("Probability")
    #ax4.set_xlabel("Number of segments")


    # Adjust layout
    plt.tight_layout()

    # Save the figure to a file
    plt.savefig("res.png")

    