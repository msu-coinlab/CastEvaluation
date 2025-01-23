from pyDOE import lhs
import json

def read_json(filename):
    # Open the JSON file
    with open(filename, 'r') as file:
        # Load the JSON data
        data = json.load(file)

    # Access the data
    amount_ = data['amount']
    lc_to_ = data['land_conversion_to']
    return amount_, lc_to_
def compute_lhs(num_samples, num_variables):
    # Generate the Latin Hypercube Samples
    samples = lhs(num_variables, samples=num_samples)

    # Print the generated samples
    for i in range(num_samples):
        print(f"Sample {i+1}: {samples[i]}")
    return samples
if __name__ == '__main__':
    # Define the number of samples
    num_samples = 10

    # Define the number of variables
    num_variables = 8
    samples = compute_lhs(num_samples, num_variables)

