# Import pandas
import pandas as pd

def reduce_rows(filename_in, filename_out):
    # Load your CSV file
    df = pd.read_csv(filename_in)

    # Group by 'CostProfileId', 'BmpId', 'AnimalId' and 'TotalCostPerUnit'
    # Keep the first entry of each group
    df = df.sort_values(['CostProfileId', 'BmpId', 'AnimalId', 'TotalCostPerUnit']).drop_duplicates(['CostProfileId', 'BmpId', 'TotalCostPerUnit'], keep='first')

    # Save the DataFrame back to CSV
    df.to_csv(filename_out, index=False)

if __name__ == '__main__':
    filename_in = 'TblCostBmpAnimal.csv'
    filename_out = 'TblCostBmpAnimal-reduced.csv'
    reduce_rows(filename_in, filename_out)