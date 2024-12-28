import numpy as np
import math

def gaussian_mechanism_gdp(data, sensitivity, mu):
    """
    Applies Gaussian Differential Privacy (GDP) to a statistical query.

    Parameters:
        data (numpy array): The dataset.
        sensitivity (float): The sensitivity of the query function.
        mu (float): The privacy parameter for GDP.

    Returns:
        noisy_result (float): The query result with Gaussian noise.
    """
    # Calculate the standard deviation of the noise
    sigma = sensitivity / mu
    
    # Compute the true mean of the data
    true_mean = np.mean(data)
    
    # Add Gaussian noise
    noise = np.random.normal(0, sigma)
    noisy_result = true_mean + noise
    
    return noisy_result

# Example usage
if __name__ == "__main__":
    # Example dataset
    dataset = np.array([160, 165, 170, 175, 180])
    
    # Sensitivity of the mean (range of the data divided by number of elements)
    sensitivity = (np.max(dataset) - np.min(dataset)) / len(dataset)
    
    # Privacy parameter (mu for GDP)
    mu = 1.0  # Adjust this value for desired privacy
    
    # Apply GDP mechanism
    noisy_mean = gaussian_mechanism_gdp(dataset, sensitivity, mu)
    print(f"Noisy Mean: {noisy_mean}")
