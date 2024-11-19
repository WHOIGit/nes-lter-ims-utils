# Removes the column name prefix from each record in the Atlantic Explorer Underway files
import argparse
from glob import glob
import os
import pandas as pd
import re

def clean_data(path):
    # read in the 1 minute SAMOS csv files
    print("Path:", path)
    for file in glob(os.path.join(path, '*.csv')):
        data = pd.read_csv(file, delimiter=',', header=None) # Specify header=None to treat all rows as data
        data = data.map(lambda x: re.sub(r'.*?:', '', str(x)))
        data.to_csv(file, index=False, header=False)

def main():
    parser = argparse.ArgumentParser(description='Fix Atlantic Explorer Underway Cruise Data for REST API.')
    parser.add_argument('path', type=str, help='Path to underway data files')

    args = parser.parse_args()
    clean_data(args.path)

if __name__ == '__main__':
    main()
