import pandas as pd
import numpy as np

def apply_laplace_randomization(file_path, epsilon, sensitivity=1):
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(file_path)

    # Ensure 'patient_id' column exists
    if 'PATIENT' not in df.columns:
        raise ValueError("The file must contain a 'PATIENT' column.")

    # Exclude 'patient_id' from columns to randomize
    columns_to_randomize = [col for col in df.columns if col != 'PATIENT']

    # Scale for Laplace noise
    scale = sensitivity / epsilon

    # Apply Laplace noise and round to the nearest positive whole number
    for column in columns_to_randomize:
        if pd.api.types.is_numeric_dtype(df[column]):
            noise = np.random.laplace(0, scale, size=df[column].shape)
            df[column] = df[column] + noise
            df[column] = df[column].round().clip(lower=0)  # Round and ensure non-negative

    return df

#=================================================================================
file_path = 'Processed/processed_medications.csv'
epsilon = 0.5  # Adjust the epsilon value as needed
#=================================================================================


randomized_df = apply_laplace_randomization(file_path, epsilon)

# Save the modified DataFrame to a new file
output_path = 'Processed/processed_medications_randomized.csv'
randomized_df.to_csv(output_path, index=False)

print(f"Randomized data saved to {output_path}")
