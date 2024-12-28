import dask.dataframe as dd
import numpy as np

def exponential_mechanism(df, utility_function, epsilon):   
    # Ensure the input DataFrame is one-hot encoded (binary columns only)
    valid_values = df[df.columns].isin([0, 1]).all().compute()

    if valid_values.all():
        print("All values in the specified columns are 0 or 1.")
    else:
        print("Some columns contain values other than 0 or 1.")

    # Compute utility scores for each column
    column_utilities = {
        col: utility_function(df[col].compute().values) for col in df.columns
    }

    # Get the maximum sensitivity (assume it's 1 for one-hot encoded values)
    sensitivity = 1

    # Calculate scores with the exponential mechanism formula
    scores = {col: np.exp(min(epsilon * utility / (2 * sensitivity),700)) for col, utility in column_utilities.items()}

    # Normalize scores into a probability distribution
    total_score = sum(scores.values())
    probabilities = {col: score / total_score for col, score in scores.items()}

    # Perform random selection based on the probability distribution
    selected_column = np.random.choice(list(probabilities.keys()), p=list(probabilities.values()))

    return selected_column

def utility_func(col_values):
    return np.sum(col_values)


if __name__ == "__main__":
    df_allergies = dd.read_csv('Processed/processed_allergies.csv')
    df_careplans = dd.read_csv('Processed/processed_careplans.csv')
    df_conditions = dd.read_csv('Processed/processed_conditions.csv')
    df_immunizations = dd.read_csv('Processed/processed_immunizations.csv')
    df_medications = dd.read_csv('Processed/processed_medications.csv')
    df_observations = dd.read_csv('Processed/processed_observations.csv')

    #df_list = {
    # "allergies": df_allergies,
    # "careplans": df_careplans,
    # "conditions": df_conditions,
    # "immunizations":df_immunizations,
    # "medications": df_medications,
    # "observations": df_observations
    # }
    df_list = {"allergies": df_allergies}

    #Privacy Budget
    epsilon = 1

    for name,df in df_list.items():
        # Remove ID value
        df_splice = df.iloc[:, 1:]

        # Apply the exponential mechanism to the entire DataFrame (if applicable)
        chosen_values = exponential_mechanism(df_splice, utility_func, epsilon)

        # Randomize values across the entire DataFrame
        randomized_df = df_splice.map_partitions(
            lambda partition: partition.apply(lambda col: np.random.permutation(col), axis=0),
            meta=df_splice
        )

        # Replace the DataFrame with its randomized values
        df_splice = randomized_df

        # Add back the ID column
        modified_df = df_splice.compute()
        modified_df.insert(0, "PATIENT", df.iloc[:, 0].compute())

        # Save the new DataFrame to a CSV file
        output_path = f"Processed/modified_{name}.csv"
        modified_df.to_csv(output_path, index=False)
        print(f"Modified dataset saved as '{output_path}'.")

    