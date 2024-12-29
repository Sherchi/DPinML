import dask.dataframe as dd
import numpy as np
from dask.diagnostics import ProgressBar

def laplace_mechanism(df, epsilon, sensitivity=1, one_hot_columns=None):
    if epsilon <= 0:
        raise ValueError("Epsilon must be greater than 0.")

    # Scale for the Laplace noise
    scale = sensitivity / epsilon

    # Apply Laplace noise to all or specified columns
    for column_name in df.columns:
        df[column_name] = df[column_name].map_partitions(
            lambda partition: partition + np.random.laplace(0, scale, len(partition)),
            meta=(column_name, 'float64')
        )

    if one_hot_columns:
        # Ensure one-hot encoding post-processing
        def process_partition(partition):
            for idx, row in partition.iterrows():
                max_col = row[one_hot_columns].idxmax()  # Find the column with the max value
                partition.loc[idx, one_hot_columns] = 0  # Set all to 0
                partition.loc[idx, max_col] = 1  # Set the max column to 1
            return partition

        df = df.map_partitions(process_partition, meta=df)

    return df

# Example usage:
if __name__ == "__main__":
    df_allergies = dd.read_csv('Processed/processed_allergies.csv')
    df_careplans = dd.read_csv('Processed/processed_careplans.csv')
    df_conditions = dd.read_csv('Processed/processed_conditions.csv')
    df_immunizations = dd.read_csv('Processed/processed_immunizations.csv')
    df_medications = dd.read_csv('Processed/processed_medications.csv')
    df_observations = dd.read_csv('Processed/processed_observations.csv')

    df_list = {
        #"allergies": df_allergies,
        "conditions": df_conditions,
        #"immunizations": df_immunizations,
    }

    for name, df in df_list.items():
        # Remove ID value
        df_splice = df.iloc[:, 1:]

        epsilon = 5

        if name == "careplans":
            one_hot_columns = list(df_splice.columns)  # Change based on what columns you want to be one-hot
            one_hot_columns.remove("Infectious disease care plan (record artifact)")  # Replace with the column to exclude
        else:
            one_hot_columns = list(df_splice.columns)  # Change based on what columns you want to be one-hot

        print("HERE")
        df_splice = laplace_mechanism(df_splice, epsilon, one_hot_columns=one_hot_columns)

        print("HERE2")
        # Add only the original ID column back to the processed dataframe
        df_combined = dd.concat([df.iloc[:, 0], df_splice], axis=1)


        # Save the noisy data back to disk
        output_path = f'Processed/noisy_{name}.csv'

        with ProgressBar():
            df_combined.to_csv(output_path, single_file=True, index=False)

    print("Laplace mechanism applied and saved to files.")
