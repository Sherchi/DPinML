import pandas as pd

def compare_tables(file1, file2):
    """
    Compare two CSV files with identical column names and compute the percentage of identical values.

    Args:
        file1 (str): Path to the first CSV file.
        file2 (str): Path to the second CSV file.

    Returns:
        float: Percentage of identical values.
    """
    # Read the CSV files into DataFrames
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Ensure both DataFrames have the same shape
    if df1.shape != df2.shape:
        raise ValueError("DataFrames must have the same shape to compare values.")

    # Ensure both DataFrames have the same column names
    if not all(df1.columns == df2.columns):
        raise ValueError("DataFrames must have identical column names.")

    # Compare values element-wise
    comparison = df1 == df2

    # Calculate the percentage of identical values
    total_elements = comparison.size
    matching_elements = comparison.sum().sum()  # Sum across both rows and columns
    percentage_match = (matching_elements / total_elements) * 100

    return percentage_match

# Example Usage
# Compare two CSV files
file1 = "Processed/modified_allergies.csv"
file2 = "Processed/processed_allergies.csv"

percentage_match = compare_tables(file1, file2)
print(f"Percentage of matching values: {percentage_match:.2f}%")
