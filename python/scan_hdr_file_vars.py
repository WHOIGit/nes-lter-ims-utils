import os
import re
import argparse
from collections import namedtuple, defaultdict

Variable = namedtuple('Variable', ['name', 'description', 'unit'])

def parse_variable_line(line):
    # Regular expression to match the variable line pattern
    pattern = r'# name \d+ = (.+?): (.+?) \[(.+?)\]'
    match = re.match(pattern, line)
    
    if match:
        name, description, unit = match.groups()
        return Variable(name.strip(), description.strip(), unit.strip())
    return None

def process_header_file(file_path, variable_counts):
    used_in_this_file = set()
    try:
        with open(file_path, 'r', encoding='latin-1') as file:
            for line in file:
                if line.startswith('# name'):
                    variable = parse_variable_line(line)
                    if variable and not variable in used_in_this_file:
                        variable_counts[variable] += 1
                        used_in_this_file.add(variable)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def scan_directory(directory):
    variable_counts = defaultdict(int)
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.hdr'):
                file_path = os.path.join(root, file)
                process_header_file(file_path, variable_counts)
    return variable_counts

def main():
    parser = argparse.ArgumentParser(description='Parse SeaBird CTD header files for variables.')
    parser.add_argument('directory', help='Directory path containing .hdr files to scan')
    args = parser.parse_args()

    variable_counts = scan_directory(args.directory)
    print('name,description,unit,file_count')
    for variable, count in variable_counts.items():
        print(f'{variable.name},"{variable.description}","{variable.unit}",{count}')

if __name__ == "__main__":
    main()
