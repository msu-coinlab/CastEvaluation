import csv
import json
import sys

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
def read_csv_with_headers_1(file_path, key_column):
    data = {}
    with open(file_path, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            key = row.pop(key_column)
            for k, v in row.items():
                try:
                    row[k] = int(v)
                except ValueError:
                    try:
                        row[k] = float(v)
                    except ValueError:
                        pass
            data[key] = list(row.values())
    return data
def read_csv_with_headers_2(file_path, key_column1, key_column2):
    data = {}
    with open(file_path, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            # Create a composed key
            county_name =  row.pop(key_column1).lower()
            if county_name != 'district of columbia':
                if not county_name.endswith('city'):
                    county_name += ' county'
            if county_name in ['charles city', 'james city']:
                county_name +=  ' county'
            if county_name.startswith('alexandria'):
                print(county_name)
            if county_name in ['alexandria county', 'buena vista county']:
                county_name = county_name.replace("county", "city")

            key = county_name + ", " + row.pop(key_column2)

            for k, v in row.items():
                try:
                    row[k] = int(v)
                except ValueError:
                    try:
                        row[k] = float(v)
                    except ValueError:
                        pass
            data[key] = list(row.values())
    return data
def read_json_data_from_file(file_path):
    with open(file_path, "r") as json_file:
        data = json.load(json_file)
    return data
def merge_dict_lists(dict1, dict2):
    for key in dict1.keys():
        if key in dict2:
            dict1[key].extend(dict2[key])
    return dict1

def print_dict(dictionary):
    for key, value in dictionary.items():
        print(f"Key: {key} -> Value: {value}")

def get_county_idx(data):
    data_idx = {}
    data_neighbors = {}
    for county_name_state, county_id, neighbors in data:
        county_name = county_name_state.lower()
        county_name = county_name.replace("'", "")
        data_idx[county_name] = county_id
        data_neighbors[county_id] = neighbors
    return data_idx, data_neighbors

def write_dict_to_json(dictionary, file_path):
    with open(file_path, 'w') as json_file:
        json.dump(dictionary, json_file)
def read_json_to_dict(file_path):
    with open(file_path, 'r') as json_file:
        dictionary = json.load(json_file)
    return dictionary

def get_neighborgs(data_idx, data_neighbors, counties_ext):
    counties = {}
    county_cast = {}
    cast_neighbors = {}
    for  key, value in counties_ext.items():
        if key in data_idx.keys():
            county_cast[data_idx[key]] = value[0]
        else:
            print('My_key', key)

    #sys.exit()
    #print(county_cast)
    for key, value in data_neighbors.items():
        if key not in county_cast.keys():
            continue
        else:
            print(key)

        tmp_lst = []
        for x in value:
            if x != key and x in county_cast.keys():
               tmp_lst.append(county_cast[x])
        cast_neighbors[county_cast[key]] = tmp_lst

    return cast_neighbors









if __name__ == '__main__':
    data = read_json_data_from_file("parsed_county_data.json")
    data_idx, data_neighbors  = get_county_idx(data)
    counties = read_csv_with_headers_2('../data/TblCounty.csv', 'CountyName', 'StateAbbreviation')
    geographies = read_csv_with_headers_1('../data/TblGeography.csv', 'GeographyFullName')
    counties_ext = merge_dict_lists(counties, geographies)
    headers = ["CountyName","CountyId","StateAbbreviation","FIPS","GrowingAreaId","FractionSoilOrganicMatter","SoilOrganicMatterPlantAvailableNPerAcre","GeographyId","GeographyTypeId","GeographyName"]
    file_path = 'salida.json'
    write_dict_to_json(counties_ext, file_path)
    counties_ext2 = read_json_to_dict(file_path)
    print_dict(counties_ext2)

    cast_neighbors = get_neighborgs(data_idx, data_neighbors, counties_ext)
    file_path = 'cast_neighbors.json'
    write_dict_to_json(cast_neighbors, file_path)
    print(cast_neighbors)

