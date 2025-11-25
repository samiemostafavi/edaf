# import pandas as pd

# # Read CSV
# df = pd.read_csv("delayCal17_updated.csv")

# # Write to Excel, keep full floating-point precision
# df.to_excel("delayCal17_updated.xlsx", index=False, float_format="%.15g")


import pandas as pd

# Read the CSV file
df = pd.read_csv("/home/wilsonan/edaf_new/edaf/nov13_results/delayCal_13112025_v1.csv")

# Keep only the first 39,800 rows
df_limited = df.iloc[:39800]

# Save to Excel
df_limited.to_excel("/home/wilsonan/edaf_new/edaf/nov13_results/delayCal_13112025_v1.xlsx", index=False,float_format="%.15g")