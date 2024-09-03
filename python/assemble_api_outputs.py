# This Python Script assembles a list of output column header names 
# for each end point for API version 1.

import argparse
import os
from io import StringIO
import re
import pandas as pd
import csv
import requests
from collections import Counter
from itertools import product
from functools import reduce

current_dir = os.getcwd()

buffer = StringIO()

def read_from_api(type, cruises):
    # Expand the cruises into a DataFrame (avoids nested for loops)
    z = pd.DataFrame(list(product(cruises)), columns=['Var1'])

    # Generate URLs based on the specified source type
    if type == "metadata":
        urls = [f"https://nes-lter-data.whoi.edu/api/ctd/{cruise}/metadata.csv" for cruise in z['Var1']]
    elif type == "summary":
        urls = [f"https://nes-lter-data.whoi.edu/api/ctd/{cruise}/bottle_summary.csv" for cruise in z['Var1']]
    elif type == "nutrient":
        urls = [f"https://nes-lter-data.whoi.edu/api/nut/{cruise}.csv" for cruise in z['Var1']]
    elif type == "chl":
        urls = [f"https://nes-lter-data.whoi.edu/api/chl/{cruise}.csv" for cruise in z['Var1']]
    elif type == "bottles":
        urls = [f"https://nes-lter-data.whoi.edu/api/ctd/{cruise}/bottles.csv" for cruise in z['Var1']]
    elif type == "stations":
        urls = [f"https://nes-lter-data.whoi.edu/api/stations/{cruise}.csv" for cruise in z['Var1']]
    elif type == "underway":
        urls = [f"https://nes-lter-data.whoi.edu/api/underway/{cruise}.csv" for cruise in z['Var1']]
    elif type == "events":
        urls = [f"https://nes-lter-data.whoi.edu/api/events/{cruise}.csv" for cruise in z['Var1']]
    elif type == "cast":
        urls = [f"https://nes-lter-data.whoi.edu/api/ctd/{cruise}/cast_2.csv" for cruise in z['Var1']] #en627 starts with cast #2
    elif type == "hplc":
        urls = [f"https://nes-lter-data.whoi.edu/api/hplc/{cruise}.csv" for cruise in z['Var1']]
    else:
        raise ValueError(f"Unknown type: {type}")

    unique_columns = set()

    # Loop through URLs to read in cruise data
    for k, url in enumerate(urls):
        print(f"Reading {url} ...")
        try:
            cruise_df = pd.read_csv(url)
            unique_columns.update(cruise_df.columns)
        except Exception as e:
            print(f"An error occurred while reading {url}: {e}")

    # Uncomment if you want list sorted alphbetically
    unique_columns = sorted(unique_columns)
    unique_columns_df = pd.DataFrame(unique_columns)

    return unique_columns_df

def assemble_outputs():
    print(f"Compiling a list of API 1 outputs for all cruises.")
    buffer.write(f"Compiling a list of API 1 outputs for all cruises.\n\n")
    cruiselist = []

    # Get a list of all the Cruises
    try:
        response = requests.get("https://nes-lter-data.whoi.edu/api/cruises")
        data = response.json()
        cruiselist = data["cruises"]
    except Exception as e:
        if e.code != 404:
            print(f"An error occurred while reading cruise list: {e}")
            exit


    # EVENTS
    print(f"Compiling outputs for Events.")
    buffer.write(f"\nCompiling outputs for Events.\n")

    # Compile cruise data from API
    api_events = read_from_api(type="events", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # METADATA
    print(f"Compiling outputs for Metadata.")
    buffer.write(f"\nCompiling outputs for Metadata.\n")

    api_events = read_from_api(type="metadata", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # BOTTLE SUMMARY
    print(f"Compiling outputs for Bottle Summary.")
    buffer.write(f"\nCompiling outputs for Bottle Summary.\n")

    api_events = read_from_api(type="summary", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")


    # NUTRIENT
    print(f"Compiling outputs for Nutrient.")
    buffer.write(f"\nCompiling outputs for Nutrient.\n")

    api_events = read_from_api(type="nutrient", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # CHLOROPHYLL
    print(f"Compiling outputs for Chlorophyll.")
    buffer.write(f"\nCompiling outputs for Chlorophyll.\n")

    api_events = read_from_api(type="chl", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # BOTTLES
    print(f"Compiling outputs for Bottles.")
    buffer.write(f"\nCompiling outputs for Bottles.\n")

    api_events = read_from_api(type="bottles", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # STATIONS
    print(f"Compiling outputs for Stations.")
    buffer.write(f"\nCompiling outputs for Stations.\n")

    api_events = read_from_api(type="stations", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # UNDERWAY
    print(f"Compiling outputs for Underway Data.")
    buffer.write(f"\nCompiling outputs for Underway Data.\n")

    api_events = read_from_api(type="underway", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # CAST
    print(f"Compiling outputs for Cast Data.")
    buffer.write(f"\nCompiling outputs for Cast Data.\n")

    api_events = read_from_api(type="cast", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    # HPLC
    print(f"Compiling outputs for HPLC Data.")
    buffer.write(f"\nCompiling outputs for HPLC Data.\n")

    api_events = read_from_api(type="hplc", cruises=cruiselist)

    print(f"Unique columns across all cruises:")
    buffer.write(f"Unique columns across all cruises:\n")
    buffer.write(api_events.to_string(header=False, index=False))
    buffer.write(f"\n")

    buffer_content = buffer.getvalue()
    buffer.close()
    with open("api_outputs.txt", "w") as file: 
        file.write(buffer_content)

def main():

    assemble_outputs()

if __name__ == '__main__':
    main()
