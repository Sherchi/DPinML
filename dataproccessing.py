import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.distributed import Client

# Define chunk size
chunk_size = 1000  # Adjust based on memory

# Function to clean columns and rename
def clean_columns(df, prefix):
    # Strip column names of any leading/trailing spaces
    df.columns = df.columns.str.strip()
    print(f"Columns in {prefix}: {df.columns.tolist()}")  # Print column names for inspection
    if 'PATIENT' not in df.columns:
        raise ValueError(f"PATIENT column missing in {prefix} dataframe")
    df = df.rename(columns=lambda x: f"{prefix}_{x}" if x != "PATIENT" else x)
    return df

# Initialize an empty Dask dataframe for merging
merged_df = dd.from_pandas(pd.DataFrame(), npartitions=1)

# Read and process the allergies dataset in chunks
chunk_list = []
for chunk in pd.read_csv('10k_synthea_covid19_csv/allergies.csv', chunksize=chunk_size):
    chunk = clean_columns(chunk, 'allergies')
    chunk_dd = dd.from_pandas(chunk, npartitions=4)  # Set appropriate partitions
    chunk_list.append(chunk_dd)

# Concatenate all chunks of allergies into a single Dask DataFrame
allergies_df = dd.concat(chunk_list, axis=0, interleave_partitions=True)

# Reset chunk_list for next file
chunk_list = []

# Repeat the merging process for all datasets
# Merging careplans, conditions, immunizations, medications, and observations

def merge_csv(file_path, prefix):
    chunk_list = []
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk = clean_columns(chunk, prefix)
        chunk_dd = dd.from_pandas(chunk, npartitions=4)
        chunk_list.append(chunk_dd)
    return dd.concat(chunk_list, axis=0, interleave_partitions=True)

careplans_df = merge_csv('10k_synthea_covid19_csv/careplans.csv', 'careplans')
merged_df = dd.merge(allergies_df, careplans_df, on="PATIENT", how="outer")
print("HERE1")


conditions_df = merge_csv('10k_synthea_covid19_csv/conditions.csv', 'conditions')
merged_df = dd.merge(merged_df, conditions_df, on="PATIENT", how="outer")
print("HERE2")
immunizations_df = merge_csv('10k_synthea_covid19_csv/immunizations.csv', 'immunizations')
merged_df = dd.merge(merged_df, immunizations_df, on="PATIENT", how="outer")
print("HERE3")

medications_df = merge_csv('10k_synthea_covid19_csv/medications.csv', 'medications')
merged_df = dd.merge(merged_df, medications_df, on="PATIENT", how="outer")
print("HERE4")


"""
observations_df = merge_csv('10k_synthea_covid19_csv/observations.csv', 'observations')
merged_df = dd.merge(merged_df, observations_df, on="PATIENT", how="outer")
print("HERE5")"""



# Adjust the partition size to reduce memory usage
merged_df = merged_df.repartition(partition_size="50MB")


# Instead of computing the entire dataframe into memory, we will write it directly to CSV
output_path = 'merged_patient_data-*.csv'  # Dask will save this as multiple CSV files


with ProgressBar():
    merged_df.to_csv(output_path, index=False, single_file=False)

    
# Export the merged DataFrame to CSV in parts

print("HERE6")

# If you want to monitor the progress or print the first few rows, compute the head
merged_df_computed = merged_df.head()
print(merged_df_computed)
