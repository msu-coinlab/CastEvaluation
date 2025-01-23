import re
import json
# https://github.com/abhirupdatta/UScounties
def parse_county_data(data):
    county_data = []

    lines = data.strip().split("\n")

    county_name_state = None
    county_id = None
    neighbors = []

    for line in lines:
        if not line.startswith("\t"):  # New county entry
            if county_name_state is not None:  # Save the previous county data
                county_data.append((county_name_state, county_id, neighbors))

            match = re.match(r'^(".*?")\s+(\d+)\s+', line)
            if match:
                county_name_state = match.group(1).strip('"')
                county_id = int(match.group(2))
                neighbors = []
        else:  # Neighboring county entry
            neighbor_id = re.search(r'\s+(\d+)$', line)
            if neighbor_id:
                neighbors.append(int(neighbor_id.group(1)))

    # Save the last county data
    if county_name_state is not None:
        county_data.append((county_name_state, county_id, neighbors))

    return county_data

def read_data_from_file(file_path):
    with open(file_path, 'r', encoding='latin-1') as file:
        data = file.read()
    return data

def convert_latin1_to_utf8(latin1_string):
    # Decode the Latin-1 string to Unicode
    unicode_string = latin1_string.encode('latin-1').decode('utf-8')
    return unicode_string

def save_to_json(data, filename):
    # Save the parsed data into a JSON file
    with open(filename, "w") as json_file:
        json.dump(data, json_file)
def read_json_data_from_file(file_path):
    with open(file_path, "r") as json_file:
        data = json.load(json_file)
    return data
def print_data(data):
    for county_name_state, county_id, neighbors in data:
        print(f"County Name and State: {county_name_state}")
        print(f"County ID: {county_id}")
        print(f"Neighboring Counties' IDs: {neighbors}")
if __name__ == "__main__":
    file_path = "../data/county_adjacency.txt"
    data = read_data_from_file(file_path)
    data_utf8 = convert_latin1_to_utf8(data)

    parsed_data = parse_county_data(data_utf8)
    save_to_json(parsed_data, "parsed_county_data.json")
    my_data = read_json_data_from_file("parsed_county_data.json")
    print_data(my_data)

