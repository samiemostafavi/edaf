import pandas as pd

# Load the CSV file
df = pd.read_csv("/home/wilsonan/edaf_new/edaf/nov13_results/delayCal_13112025_v1.csv")   # replace with your file name

# Choose the column you want to check
column_name = "Retransmission delay"
# Count values greater than 0
count = (df[column_name] > 0).sum()

print(f"Number of values greater than 0 in column '{column_name}': {count}")

#average_delay = df["e2e_delays"].mean()
average_delay = df["End to End Delay"].mean()
print(f"Average End-to-End Delay: {average_delay:.2f}")

# Delay measurements 
# Exp No.       No. of Retx          Avg. e2e delay (ms)
#   1               9                      18.76
#   2               15                     18.75
#   3               7                      18.73
#   4               4                      18.79
#   5               18                     18.87
#   6               131                    19.02
#   7               6720                   20.57
#   8               14539                  21.57
#   9               2                      11.71
#   10              6                      14.36
#   11              18                     16.29
#   12              10                     18.73
#   13              57                     18.84
#   14              56                     18.78
#   15              142                    18.95
#   16              99                     18.92
#   17              164                    19.07
#   18              83                     18.95
#   19              5763                   43.75
#   20              5169                   47.67
