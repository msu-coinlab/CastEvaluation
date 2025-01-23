import csv
import json
import sys
import openpyxl
import pandas as pd
import json

def read_csv_with_headers(file_path):
    data = []
    with open(file_path, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            for k, v in row.items():
                try:
                    row[k] = int(v)
                except ValueError:
                    try:
                        row[k] = float(v)
                    except ValueError:
                        pass
            data.append(row)
    return data


def compute_excess(excel_path, sheet_name):
    df_nutrients_read = pd.read_excel(excel_path, sheet_name=sheet_name, engine='openpyxl')

    # Create the "NApplied" column by summing the specified columns
    columns_to_sum = [
        'ManureNLbsApplied', 'BiosolidsNLbsApplied', 'FertilizerNLbsApplied',
        'DirectDepositManureNLbsApplied', 'UrbanFertilizerNLbsApplied',
        'TotalNApplication', 'TotalNApplicationPerAcre', 'TotalNApptoNCropNeed',
        'LegumeNLbsFixed'
    ]
    
    df_nutrients_read['NApplied'] = df_nutrients_read[columns_to_sum].sum(axis=1)
    
    # Create the "NExcess" column by subtracting "NCropNeed" from "NApplied"
    df_nutrients_read['NExcess'] = df_nutrients_read['NApplied'] - df_nutrients_read['NCropNeed']
    
    # Show the modified DataFrame
    return df_nutrients_read



def nutrient_applied_by_year(filename_in, year):
    df_nutrients_read = compute_excess(filename_in, 'Nutrients Applied')
    #df_nutrients_read['Geography'] = ['Jefferson, WV', 'Jefferson, WV', 'Washington, DC']

    # Group by the "Geography" column and sum the "NExcess" values for each group
    grouped_df = df_nutrients_read.groupby('Geography')['NExcess'].sum().reset_index()

    # Convert the "Geography" and "SumNExcess" columns to a dictionary
    data_dict = dict(zip(grouped_df['Geography'], grouped_df['NExcess']))
    
    
    # Include the year as a general key
    final_dict = {year: data_dict}
    return final_dict
    

if __name__ == '__main__':
    filename_in = 'ChesapeakeBay-NutrientsApplied.xlsx'
    year = 2019
    json_file = nutrient_applied_by_year(filename_in, year)
    # Specify the JSON file path
    json_path_with_year = '2019-nexcess.json'
    # Write the dictionary to a JSON file
    with open(json_path_with_year, 'w') as f:
        json.dump(json_file, f)

