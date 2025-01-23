import pandas as pd

def process_df(df, old_ids, new_id):
    # Filter the rows where AnimalId belongs to old_ids
    filtered_df = df[df['AnimalId'].isin(old_ids)]

    # Group by 'BaseConditionId', 'CountyId', 'LoadSourceId' and sum 'AnimalUnits'
    grouped_df = filtered_df.groupby(['BaseConditionId', 'CountyId', 'LoadSourceId'], as_index=False).sum()

    # Create a new DataFrame with the summed up 'AnimalUnits' and 'AnimalId'=new_id
    new_df = pd.DataFrame({'BaseConditionId': grouped_df['BaseConditionId'],
                           'CountyId': grouped_df['CountyId'],
                           'LoadSourceId': grouped_df['LoadSourceId'],
                           'AnimalId': new_id,
                           'AnimalUnits': grouped_df['AnimalUnits']})

    # Drop the rows in the original DataFrame where 'AnimalId' belongs to old_ids
    df = df[~df['AnimalId'].isin(old_ids)]

    # Append the new DataFrame to the original DataFrame
    df = df.append(new_df)

    return df

if __name__ == '__main__':

    # Load your CSV file
    df = pd.read_csv('TblAnimalPopulation.csv')

    # Drop the unwanted column
    df = df.drop(columns='AnimalCount')
    # Process the DataFrame for the first set of AnimalIds and new AnimalId=17
    df = process_df(df, [1,2,5,9], 17)

    # Process the DataFrame for the second set of AnimalIds and new AnimalId=20
    df = process_df(df, [3,4,6,7,8,10,11,12], 20)

    # Save the DataFrame back to CSV
    df.to_csv('your_file.csv', index=False)