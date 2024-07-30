# This Python Script reads the attributes from the _info excel file in each LTER EDI package in
# https://github.com/topics/nes-lter-edi-packages
# and compiles a list of all attributes defined and saves them to a csv file.

import argparse
import os
from io import StringIO
import re
import pandas as pd
import csv

import fnmatch
import requests
import tempfile

GITHUB_API_URL = "https://api.github.com"
GITHUB_TOKEN = ""  # Replace with your GitHub token
output_file = 'all-lter-attributes.txt'

current_dir = os.getcwd()

buffer = StringIO()

def get_repos_by_topic(topic, token):
    url = f"{GITHUB_API_URL}/search/repositories?q=topic:{topic}"
    headers = {
        "Authorization": f"token {token}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["items"]

def get_repo_content(owner, repo, path="", token=""):
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/contents/{path}"
    headers = {
        "Authorization": f"token {token}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def find_data_files(owner, repo, token, file_patterns):
    matches = []
    stack = [""]
    
    while stack:
        current_path = stack.pop()
        contents = get_repo_content(owner, repo, current_path, token)
        
        for item in contents:
            if item['type'] == 'dir':
                stack.append(item['path'])
            elif item['type'] == 'file':
               for pattern in file_patterns:
                    if fnmatch.fnmatch(item['name'], pattern):
                        matches.append(item['download_url'])
                        break
    
    return matches

def read_data_file(url):
    df = pd.DataFrame()
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
        temp_file.write(response.content)
        temp_file_path = temp_file.name
    
    if url.endswith(".xlsx"):
            try:
                df = pd.read_excel(temp_file_path, sheet_name='ColumnHeaders')
            except ValueError as e:
                try:
                    df = pd.read_excel(temp_file_path, sheet_name='ColumnHeadersGop') #eims-toi-ncp-gop
                    df1 = pd.read_excel(temp_file_path, sheet_name='ColumnHeadersNcp')
                except ValueError as e:
                    try:
                        df = pd.read_excel(temp_file_path, sheet_name='ColumnHeadersCount') #trawl-transect-abundance
                        df1 = pd.read_excel(temp_file_path, sheet_name='ColumnHeadersMeta')
                    except ValueError as e:
                        try:
                            df = pd.read_excel(temp_file_path, sheet_name='ColumnHeadersDiscrete') #npp-transect
                            df1 = pd.read_excel(temp_file_path, sheet_name='ColumnHeadersIntegrated')
                            df2 = pd.read_excel(temp_file_path, sheet_name='ColumnHeadersPOC')
                        except ValueError as e:
                            if 'Worksheet named' in str(e):
                                print(f"Sheet 'ColumnHeaders' does not exist in the file: {url}")
                            else:
                                raise
                            return None, None, None
            except Exception as e:
                print(f"An error occurred while processing the file: {url}")
                return None, None, None
            finally:
                os.remove(temp_file_path)
                
    else:   #attributes txt files
        try:
            df = pd.read_csv(url, sep='\t')
        except Exception as e:
            print(f"An error occurred while processing the file: {url}")
            return None, None, None
        finally:
            os.remove(temp_file_path)
        
    return df, df1, df2

def write_or_append_to_file(df, file_path):
    if not os.path.exists(file_path):
        df.to_csv(file_path, index=False, sep='\t')
        print(f"File {file_path} created and data written.")
    else:
        existing_df = pd.read_csv(file_path, sep='\t')
        combined_df = pd.concat([existing_df, df])
        combined_df.to_csv(file_path, index=False, sep='\t')

def find_files():
    
    # GitHub repository details
    owner = "joannekoch"
    topic = "nes-lter-edi-packages"
    
    matches = []
    stack = [""]

    # Get repositories by topic
    repos = get_repos_by_topic(topic, GITHUB_TOKEN)

    # Search each repository for matching files
    for repo in repos:
        owner = repo["owner"]["login"]
        repo_name = repo["name"]
        print(f"Searching in repository: {owner}/{repo_name}")
        if repo_name == "nes-lter-fish-diet-isotope":
            matching_files = find_data_files(owner, repo_name, GITHUB_TOKEN, ["*_Final.xlsx"])
        elif repo_name == "nes-lter-ifcb-transect-winter-2018":
            matching_files = find_data_files(owner, repo_name, GITHUB_TOKEN, ["*_edi.xlsx"])
        elif repo_name == "nes-lter-zooplankton-transect-inventory":
            matching_files = find_data_files(owner, repo_name, GITHUB_TOKEN, ["*_Inventory.xlsx"])
        elif repo_name == "nes-lter-chl-transect-underway-discrete":
            matching_files = find_data_files(owner, repo_name, GITHUB_TOKEN, ["attributes_*.txt"])
        elif repo_name == "nes-lter-chl-mvco":
            matching_files = find_data_files(owner, repo_name, GITHUB_TOKEN, ["attributes_*.txt"])
        else:
            matching_files = find_data_files(owner, repo_name, GITHUB_TOKEN, ["*_Info.xlsx", "*-info.xlsx"])
        if len(matching_files) == 0:
            print(f"NO INFO XLSX FILE FOUND")
            # Look for attributes files
            matching_files = find_data_files(owner, repo_name, GITHUB_TOKEN, ["attributes_*.txt"])
        for file in matching_files:
            print(f"Found: {file} in {owner}/{repo_name}")
            df, df1, df2 = read_data_file(file)
            if df is not None:
                # Add repo column
                df['repo'] = repo_name
                write_or_append_to_file(df, output_file)           
            if df1 is not None:
                # Add repo column
                df1['repo'] = repo_name
                write_or_append_to_file(df1, output_file)
            if df2 is not None:
                # Add repo column
                df2['repo'] = repo_name
                write_or_append_to_file(df2, output_file)
            
            
    return matching_files

def find_attributes():
    print(f"Compiling all LTER Attributes.")
    try:
        os.remove(output_file)
    except:
        print(f"No all-lter-attributes.txt file to delete")
        
    files = find_files()
    if len(files) == 0:
        print(f"There are no info attributes files to read.")
    else:
        df = pd.read_csv(output_file, sep='\t')
        
        #drop the duplicates and aggregate the repos
        df = df.groupby(
            ['attributeName', 'attributeDefinition'],
            as_index=False
        ).agg({
            'class': 'first',
            'unit': 'first',
            'dateTimeFormatString': 'first',
            'missingValueCode': 'first',
            'missingValueCodeExplanation': 'first',
            'repo': lambda x: ','.join(sorted(set(x)))
        })

        # Sort the DataFrame by the 'attributeName' column while preserving the first column
        sorted_df = df.sort_values(by='attributeName')

        # Save the sorted DataFrame back to the file
        sorted_df.to_csv(output_file, index=False, sep='\t')

def main():

    find_attributes()

if __name__ == '__main__':
    main()
