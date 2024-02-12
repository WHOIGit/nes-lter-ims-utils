# Protocol for reviewing processed CTD header data prior to upload to RDS for NES-LTER REST API
# Plot Function - reads .asc files and plots the data

import argparse
import os
from io import StringIO
import re
from glob import glob
import matplotlib.pyplot as plt
import pandas as pd

buffer = StringIO()
current_dir = os.getcwd()
errors_found = False

def find_asc_files(url):
    global errors_found
    matching_files = []
    for file in glob(os.path.join(url, '*.asc')):
        # get base filename
        filename = os.path.splitext(os.path.basename(file))[0]
        if "_u" not in filename and not (filename.startswith("dar") or filename.startswith("uar")): 
                matching_files.append(file)
    return matching_files

def read_data(file):
    df = pd.read_csv(file, sep=';', encoding='latin-1')
    # AR cruises have fixed width delimeters
    if len(df.columns) == 1:
        df = pd.read_fwf(file, skiprows=1, nrows=1, header=None, encoding='latin-1')
        n_cols = len(df.columns)  
        # now get the length of the first line which contains headers
        with open(file, encoding='latin-1') as fin:
            for line in fin.readlines():
                break
        # assume all columns are the same width. determine that width
        line = line.rstrip()
        col_width = int(len(line) / n_cols)
        col_widths = [col_width for _ in range(n_cols)]
        # now parse the fixed-width format
        # Pandas will automatically append ".1" to any duplicate column name
        df = pd.read_fwf(file, widths=col_widths, encoding='latin-1')
        
    return df

def plot_data(primary_sensor, secondary_sensor, cruise_name, cast, df):   
    # create the plot figure
    plt.figure(figsize=(30, 15))

    plt.plot(df[primary_sensor], df['DepSM'], c='blue', marker='o', linestyle='-')
    if secondary_sensor in df.columns:
       plt.plot(df[secondary_sensor], df['DepSM'], c='red', marker='o', linestyle='-')
       plt.text(0.5, -0.1, secondary_sensor, color='red', transform=plt.gca().transAxes, fontsize=12)
       plt.title(f'Depth vs {primary_sensor} & {secondary_sensor} for CTD Cast {cast} on Cruise {cruise_name}')
    else:
       plt.title(f'Depth vs {primary_sensor} for CTD Cast {cast} on Cruise {cruise_name}')
       plt.text(0.5, -0.1, f'No {secondary_sensor} data to plot', color='red', transform=plt.gca().transAxes, fontsize=12)
       
    plt.text(0.35, -0.1, primary_sensor, color='blue', transform=plt.gca().transAxes, fontsize=12)    
    plt.ylabel(f'Depth [salt water, m]')
    
    plt.gca().invert_yaxis()  # Invert y-axis to show increasing pressure from bottom to top
    if '/' in primary_sensor:
        primary_sensor = primary_sensor.replace('/', '')
    # save the plot to the plot files directory
    plt.savefig(current_dir + "/plot_files/" + cruise_name + "_" + cast + "_" + primary_sensor + '_plot.png')
    plt.close()
    
def plot_diff(primary_sensor, secondary_sensor, cruise_name, cast, df, args):  
    # create the plot figure
    plt.figure(figsize=(30, 15))

    # specify range of values on the x-axis
    if primary_sensor.startswith("T09"):
        plt.xlim(args.temp_min, args.temp_max) 
    elif primary_sensor.startswith("Sal"):
        plt.xlim(args.sal_min, args.sal_max) 
    elif primary_sensor.startswith("C0S"):
        plt.xlim(args.cond_min, args.cond_max) 
    elif primary_sensor.startswith("Sb"):
        plt.xlim(args.oxy_min, args.oxy_max) 

    # compute the difference between the two sensors
    diff = df[secondary_sensor] - df[primary_sensor]
    plt.plot(diff, df['DepSM'], c='blue', marker='o', linestyle='-')
    plt.title(f'Depth vs Sensor Difference {secondary_sensor} minus {primary_sensor} for CTD Cast {cast} on Cruise {cruise_name}')
       
    plt.text(0.35, -0.1, f'Sensor Difference - {secondary_sensor} minus {primary_sensor}', color='blue', transform=plt.gca().transAxes, fontsize=12)    
    plt.ylabel(f'Depth [salt water, m]')

    plt.gca().invert_yaxis()  # Invert y-axis to show increasing pressure from bottom to top
    if '/' in primary_sensor:
        primary_sensor = primary_sensor.replace('/', '')
    # save the plot to the plot_files directory
    plt.savefig(current_dir + "/plot_files/" + cruise_name + "_" + cast + "_" + primary_sensor + '_diff_plot.png')
    plt.close()
    
def temp_cond_diff(cruise_name, args):
    global errors_found
    asc_file_path = args.path
    files = find_asc_files(asc_file_path)
    if len(files) == 0:
        print(f"There are no .asc files to check for this cruise.")
        buffer.write(f"There are no .asc files to check for this cruise.\n")
        errors_found = True
        
    for file in files:           
        df = read_data(file)
        
        base_filename = os.path.basename(file)
        parts = base_filename.split('_')
        cast = parts[1].split('.')[0]
        
        if "T090C" and "C0S/m" in df.columns:
            temp_diff = df["T190C"] - df["T090C"]
            cond_diff = df["C1S/m"] - df["C0S/m"]
            
            fig, ax1 = plt.subplots(figsize=(30, 15))

            ax1.plot(cond_diff, df['DepSM'], c='red', marker='o', linestyle='-')
            ax1.set_xlabel(f'Conductivity Difference 2 - 1 [S/m]', color='red', fontsize=12)
            ax1.set_xlim(args.cond_min, args.cond_max)
            ax1.set_ylabel(f'Depth [salt water, m]')
            ax1.tick_params('x', colors='red')

            # Create a second y-axis sharing the same x-axis
            ax2 = ax1.twiny()

            # Plot the second set of data on the top x-axis
            ax2.plot(temp_diff, df['DepSM'], c='blue', marker='o', linestyle='-')
            ax2.set_xlabel(f'Temperature Difference [ITS-90, deg C]', color='blue', fontsize=12)
            ax2.set_xlim(args.temp_min, args.temp_max)
            ax2.set_ylabel(f'Depth [salt water, m]')
            ax2.tick_params('x', colors='blue')

            # Show the plot
            #plt.show()

            plt.title(f'Depth vs Temperature and Conductivity Differences for CTD Cast {cast} on Cruise {cruise_name}', c = 'black')
    
            plt.gca().invert_yaxis()  # Invert y-axis to show increasing pressure from bottom to top
 
            # save the plot to the plot_files dir
            plt.savefig(current_dir + "/plot_files/" + cruise_name + "_" + cast + "_" + 'temp_cond_diff_plot.png')
            plt.close()

    
def get_asc_data(primary_sensor, secondary_sensor, cruise_name, args):
    global errors_found
    file_path = args.path
    files = find_asc_files(file_path)
    if len(files) == 0:
        print(f"There are no .asc files to check for this cruise.")
        buffer.write(f"There are no .asc files to check for this cruise.\n")
        errors_found = True
        
    # Read the data for each cast and plot each cast separately    
    for file in files:           
        df = read_data(file)
 
        if primary_sensor in df.columns:
            base_filename = os.path.basename(file)
            parts = base_filename.split('_')
            cast = parts[1].split('.')[0]
            plot_data(primary_sensor, secondary_sensor, cruise_name, cast, df)
            if secondary_sensor in df.columns:
                plot_diff(primary_sensor, secondary_sensor, cruise_name, cast, df, args)
        else:
            print(f"{primary_sensor} column does not exist in .asc file. No plot will be generated.")
            buffer.write(f"{primary_sensor} column does not exist in .asc file. No plot will be generated.\n")
            errors_found = True

def review_data(args):
    global errors_found
    asc_file_path = args.path
    primary_sensor_list = []
    secondary_sensors = {}
    
    # create plot_files directory to store the plots
    if not os.path.exists(current_dir + "/plot_files"):
        os.makedirs(current_dir + "/plot_files")
    
    # for Sal00.1, Sal11.1 - AR cruises use the second set, EN cruises only have the first set
    ar_primary_sensor_list = ["T090C", "Sal00.1", "C0S/m", "Sbeox0ML/L" ] #AR sensors
    ar_secondary_sensors = {
        "T090C":"T190C", 
        "Sal00.1":"Sal11.1",
        "C0S/m": "C1S/m", 
        "Sbeox0ML/L": "Sbeox1ML/L"}
    en_primary_sensor_list = ["T090C", "Sal00", "C0S/m", "Sbox0Mm/Kg" ] #EN sensors
    en_secondary_sensors = {
        "T090C":"T190C", 
        "Sal00":"Sal11",
        "C0S/m": "C1S/m", 
        "Sbox0Mm/Kg": "Sbox1Mm/Kg"}
    
    match = re.search(r'(AR|EN)\w*', asc_file_path)
    if match:
        cruise_name = match.group(0)
    else:
        print("Cruise name pattern not found in file path.")
        buffer.write(f"Cruise name pattern not found in file path.\n")
        errors_found = True
        
    if cruise_name.startswith('AR'):
        primary_sensor_list = ar_primary_sensor_list
        secondary_sensors = ar_secondary_sensors
    else:  # cruise starts with EN
        primary_sensor_list = en_primary_sensor_list
        secondary_sensors = en_secondary_sensors
    
    print(f"Plotting data for processed .asc file")
    buffer.write(f"Plotting data for processed .asc file\n")    
    print(f"Plotting sensors: {primary_sensor_list}")
    buffer.write(f"Plotting sensors: {primary_sensor_list}\n")
    
    # For each sensor in the list
    for primary_sensor in primary_sensor_list:
        # Get the asc data for a sensor variable
        get_asc_data(primary_sensor, secondary_sensors[primary_sensor], cruise_name, args)    
        
    #Plot Temperature Difference vs Conductivity Difference
    temp_cond_diff(cruise_name, args)
        
    if not errors_found:
        print(f"NO ERRORS FOUND.")
        buffer.write(f"\nNO ERRORS FOUND.")
    else:
        print(f"ERRORS FOUND!")
        buffer.write(f"\nERRORS FOUND!")

    buffer_content = buffer.getvalue()
    buffer.close()
    with open(current_dir + "/" + cruise_name +"_ctd_plot_results.txt", "w") as file:
        file.write(buffer_content)
    

def main():
    # If paths are on Google Docs, you must download the files to your local pc file system.
    parser = argparse.ArgumentParser(description='Review CTD header data prior to upload to RDS for NES-LTER REST API.')
    parser.add_argument('path', type=str, help='Path to CTD processed header directory')
    parser.add_argument('temp_min', type=float, help='Temperature Min Range in Plot Scale')
    parser.add_argument('temp_max', type=float, help='Temperature Mas Range in Plot Scale')
    parser.add_argument('cond_min', type=float, help='Conductivity Min Range in Plot Scale')
    parser.add_argument('cond_max', type=float, help='Conductivity Max Range in Plot Scale')
    parser.add_argument('sal_min', type=float, help='Salinity Min Range in Plot Scale')
    parser.add_argument('sal_max', type=float, help='Salinity Max Range in Plot Scale')
    parser.add_argument('oxy_min', type=float, help='Oxygen Min Range in Plot Scale')
    parser.add_argument('oxy_max', type=float, help='Oxygen Max Range in Plot Scale')
    
    args = parser.parse_args()
    
    review_data(args)

if __name__ == '__main__':
    main()
