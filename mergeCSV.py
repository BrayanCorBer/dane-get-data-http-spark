import pandas as pd

# Read the CSV files into DataFrames
df2 = pd.read_csv('./data.csv')
df1 = pd.read_csv('./dataframe_Instituciones.csv')


# Perform the join operation
merged_df = pd.merge(df1, df2)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('output.csv', index=False)
