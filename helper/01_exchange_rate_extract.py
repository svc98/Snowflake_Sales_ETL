import os
import csv
import tempfile
import pandas as pd
from pathlib import Path

root = str(Path(__file__).parent.parent)
historical_data = root + "/helper/exchange_rate_raw"
historical_data_output = root + "/data/exchange_rate"


# Extract first 2 columns from original dataset
temp_file_paths = []
for root, dirs, files in os.walk(historical_data):
    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(root + "/" + file, usecols=[0, 1], header=0)
            df = df.map(lambda x: x.strip('"') if isinstance(x, str) else x)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                df.to_csv(temp_file.name, index=False)
                temp_file_paths.append(temp_file.name)

# Create DF's from temp files (STATIC)
df1 = pd.read_csv(temp_file_paths[0])
df2 = pd.read_csv(temp_file_paths[1])

# Transform andCombine into 1 DF
df1 = df1.rename(columns={"Date": "date", "Price": "usd2jpy"})
df1.insert(loc=1, column="usd2usd", value=1)
df2 = df2.drop("Date",axis=1)
df2 = df2.rename(columns={"Price": "usd2eur"})
combined_df = pd.concat([df1, df2], axis=1)
print(combined_df)

# Fill missing Dates and Rates
combined_df['date'] = pd.to_datetime(combined_df['date'], format="%m/%d/%Y")
combined_df.set_index('date', inplace=True)
full_date_range = pd.date_range(start=combined_df.index.min(), end=combined_df.index.max(), freq='D')
combined_df = combined_df.reindex(full_date_range)
combined_df.ffill(inplace=True)
combined_df.reset_index(inplace=True)
combined_df.rename(columns={'index': 'date'}, inplace=True)

# Save the Combined Data Frame to a  newCSV file
output_file = historical_data_output+'/exchange_rate.csv'
combined_df.to_csv(output_file, index=False)


print("Generated Exchange Rate CSV")