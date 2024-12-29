import pandas as pd
import numpy as np

def apply_laplace_mechanism(value, epsilon, sensitivity):
    scale = sensitivity / epsilon
    noise = np.random.laplace(0, scale)
    return value + noise

def apply_exponential_mechanism(value, options, epsilon, value_counts):
    # Calculate scores based on frequencies
    frequencies = np.array([value_counts[opt] for opt in options])
    probabilities = np.exp(epsilon * frequencies / 2)
    probabilities /= probabilities.sum()
    return np.random.choice(options, p=probabilities)

def process_csv(file_path, epsilon_laplace, epsilon_exponential, sensitivity=1):
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(file_path)

    # Replace 0s with N/A
    df.replace(0, np.nan, inplace=True)
    df.replace('0', np.nan, inplace=True)


    # Identify numeric and categorical columns
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_columns = [col for col in df.columns if col not in numeric_columns]

    # Ignore the first column
    if len(df.columns) > 0:
        first_column = df.columns[0]
        if first_column in numeric_columns:
            numeric_columns.remove(first_column)
        elif first_column in categorical_columns:
            categorical_columns.remove(first_column)

    # Apply Laplace mechanism to numeric columns
    for column in numeric_columns:
        df[column] = df[column].apply(
            lambda x: apply_laplace_mechanism(x, epsilon_laplace, sensitivity) if not pd.isna(x) else x
        )

    # Apply Exponential mechanism to categorical columns
    for column in categorical_columns:
        unique_values = df[column].dropna().unique()  # Exclude N/A values
        value_counts = df[column].value_counts(normalize=True).to_dict()  # Get frequency percentages
        print(f"Column '{column}' categories before applying exponential mechanism: {unique_values}")
        if len(unique_values) > 1:
            df[column] = df[column].apply(
                lambda x: apply_exponential_mechanism(x, unique_values, epsilon_exponential, value_counts) if not pd.isna(x) else x
            )

    return df

file_path = 'Processed/processed_observations.csv'

#=================================================================================
epsilon_laplace = 0.5  # Adjust the Laplace epsilon value
epsilon_exponential = 1.0  # Adjust the Exponential Mechanism epsilon value
#=================================================================================

processed_df = process_csv(file_path, epsilon_laplace, epsilon_exponential)

# Save the processed DataFrame to a new file
output_path = 'Processed/processed_observations_anonymized.csv'
processed_df.to_csv(output_path, index=False)

print(f"Processed data saved to {output_path}")
