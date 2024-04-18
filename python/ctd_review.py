# Protocol for reviewing processed CTD data prior to upload to RDS for NES-LTER REST API
# Open xmlcon file, step through each sensor, comparing cal values to respective values in cal files.

import argparse
from ast import Try
import xml.etree.ElementTree as ET
import urllib.request
from urllib.parse import urlparse, urlunparse
import os
import requests
import re
from bs4 import BeautifulSoup
from xmldiff import main as xmldiff_main, formatting
import fitz  # PyMuPDF
from io import StringIO

from glob import glob
from datetime import datetime

summary = StringIO()
buffer = StringIO()

current_dir = os.getcwd()

failed_instruments = []

def get_date_from_filename(file_name):
    date_str = file_name.split("_")[-1].split(".")[0]
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        date_pattern = re.compile(r'(\d{2}[A-Za-z]{3}\d{2})')   #date formats DDMMMYY
        match = date_pattern.search(file_name)
        if match:
            # Extract the date string from the filename
            date_str = match.group(1)
            # Convert the date string to a datetime object
            return datetime.strptime(date_str, '%d%b%y')
        else:
            # Return a default value if no date is found
            return datetime.min
        
def search_calib_files(file_names, serial_number, primary):
    matching_files = []
    for file_name in file_names:
        if file_name.lower().endswith(".xml") and str(serial_number) in file_name and '_Repair' not in file_name:  #prefer to use xml files
            if primary:
                matching_files.append("primary_ctd_cals\\" + file_name)
            else:
                matching_files.append(file_name)
    if len(matching_files) == 0:
        for file_name in file_names:    
            if file_name.lower().endswith(".pdf") and str(serial_number) in file_name and '_Repair' not in file_name:
                if primary:
                    matching_files.append("primary_ctd_cals\\" + file_name)
                else:
                    matching_files.append(file_name)
    return matching_files

def find_calib_file_with_serial_number(url, serial_number):
    index = serial_number.find('s/n')
    if index != -1:
      serial_number = serial_number[index + 4:].strip()

    # look in the primary_ctd_cals folder first
    primary_ctd_dir = url + "/primary_ctd_cals"
    if os.path.exists(primary_ctd_dir):
        file_names = os.listdir(primary_ctd_dir)
        matching_files = search_calib_files(file_names, serial_number, True)
    else:
        file_names = os.listdir(url)
        # if file_name is a directory name
        for file_name in file_names:
            new_path = os.path.join(url, file_name)
            if os.path.isdir(new_path) and str(serial_number) in file_name:
                os.chdir(new_path)
                url = new_path
                file_names = os.listdir(new_path)
                
        matching_files = search_calib_files(file_names, serial_number, False)

    if not matching_files:
        return None
    
    # pick the latest date if more than one filename matches
    newest_file = max(matching_files, key=get_date_from_filename)
    file_url = os.path.join(url, newest_file)
    file_url = file_url.replace("\\", "/")
    return file_url


def find_xmlcon_files(url):
    matching_files = []
    for file in glob(os.path.join(url, '*.XMLCON')):
        if 'L011B11' not in file:     #en668
            matching_files.append(file)
    return matching_files

def get_data(file):
    # Fetch the xmlcon data from the URL
    try:
        response = urllib.request.urlopen(file)
        xml_data = response.read()
        # Parse the xmlcon data
        return ET.fromstring(xml_data)
    except:
    # Fetch the xmlcon data from local folder
       if file.lower().endswith(".xml") or file.lower().endswith(".xmlcon"):
           with open(file, "r", encoding="utf-8") as file:
               tree = ET.parse(file)
               return tree.getroot()
       elif file.lower().endswith(".pdf"):
           text = ""
           try:
               with fitz.open(file) as pdf_document:     #local file
                   for page_num in range(pdf_document.page_count):
                       page = pdf_document[page_num]
                       text += page.get_text()
           except:
               response = requests.get(file)           # https file
               if response.status_code == 200:
                  with fitz.open("pdf", response.content) as pdf_document:                  
                      for page_num in range(pdf_document.page_count):
                          page = pdf_document[page_num]
                          text += page.get_text()
           if text == "":
               buffer.write(f"PDF file is NOT in machine readable format: {file}\n")
           return text
                         
    
def get_doc_dir(xmlcon_file_path):
    # get the ctd/doc dir name for this cruise
    url_parts = list(urlparse(xmlcon_file_path))

    # Remove the last component (file name) from the path and append /doc
    url_parts_path = '/'.join(url_parts[2].split('/')[:-1]) + '/doc/'
    
    url_parts[2] = url_parts_path
    return urlunparse(url_parts)

def check_coefficients(xmlcon_root, sensor_element, sensor_root, serial_number):
    for element in sensor_element:
        buffer.write(f"{element.tag}: {element.text}\n")
        process_each_element(xmlcon_root, element, sensor_root, serial_number)

def process_each_element(xmlcon_root, element, sensor_root, serial_number):
    calib_elements = find_calibration_elements(xmlcon_root, element.tag)
    calib_element = find_element_by_serial(calib_elements, serial_number)
    print(f"serial number: {serial_number}")
    check_calib_elements(calib_element, sensor_root, serial_number)

def check_calib_elements(calib_element, sensor_root, serial_number):
    coef_index = 0  # coefficient tag equation index
    for calib in calib_element:
        if calib.tag == 'Coefficients' or calib.tag == 'CalibrationCoefficients':
            buffer.write(f"Checking {calib.tag}: {calib.attrib}\n")
            check_calibration_tags(calib, sensor_root, coef_index, serial_number)
            coef_index += 1

def check_calibration_tags(calib, sensor_root, coef_index, serial_number):
    tag_occurrences = sensor_root.findall('.//' + calib.tag)
    if tag_occurrences:
        tag = tag_occurrences[coef_index] if len(tag_occurrences) > coef_index else None
        compare_tags(calib, tag, serial_number)
        check_coefficient_details(calib, sensor_root, coef_index, serial_number)

def compare_tags(calib, tag, serial_number):
    if calib.attrib == tag.attrib:
        buffer.write(f"Calibration Value check passed\n")
    else:
        buffer.write(f"Check FAILED: calibration {calib.tag} and {calib.attrib} do not match values in calib file: {tag.attrib}\n")
        record_failure(serial_number)

def check_coefficient_details(calib, sensor_root, coef_index, serial_number):
    coef_elements = calib.findall('.//')
    for coef in coef_elements:
        buffer.write(f"Checking {coef.tag}: {coef.text}\n")
        ctags = sensor_root.findall('.//' + coef.tag)
        process_coefficient_matches(ctags, coef, coef_index, serial_number)

def process_coefficient_matches(ctags, coef, coef_index, serial_number):
    ctag = ctags[coef_index] if len(ctags) > 1 else ctags[0] if ctags else None
    print(f"len(ctags) {len(ctags)}")
    print(f"ctag {ctag}")
    print(f"coef_index {coef_index}")
    if float(coef.text) == float(ctag.text):
        buffer.write(f"Calibration Value check passed\n")
    else:
        buffer.write(f"Check FAILED: calibration {coef.tag} and {coef.text} do not match values in calib file: {ctag.text}\n")
        record_failure(serial_number)

def record_failure(serial_number):
    if serial_number not in failed_instruments:
        failed_instruments.append(serial_number)
                
def check_date(date):
    date_formats = [
        "%d-%b-%y",
        "%d-%b-%Y",
        "%m/%d/%Y",
        "%Y%m%d",
        "%B %d, %Y",
        "%Y-%b-%d"
    ]
    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date, date_format)
            # Successfully parsed, format and return the date
            month = str(int(parsed_date.strftime("%m")))  # Remove leading zero
            day = str(int(parsed_date.strftime("%d")))    # Remove leading zero
            year = parsed_date.strftime("%Y")            # Keep four-digit year
            return f"{month}/{day}/{year}"               # Return formatted date
        except:
            continue  # Try the next format if the current one fails

    # If all formats fail, return an empty string
    return ''

def check_calibrations(xmlcon_root, sensor_element, sensor_root, serial_number ):
    # For each element in the sensor element (TemperatureSensor, ConductivitySensor, PressureSensor, etc)
    for element in sensor_element:
        buffer.write(f"{element.tag}: {element.text}\n")
        calib_elements = find_calibration_elements(xmlcon_root, element.tag)
        calib_element = find_element_by_serial(calib_elements, serial_number)

        # for each calibration tag in sensor
        for calib in calib_element:    
            # Process Coefficients separately below
            if calib.tag not in ['SerialNumber', 'CalibrationDate', 'Coefficients', 'CalibrationCoefficients']:
                tag = sensor_root.find('.//' + calib.tag)
                # check if calibration tag and text in xmlcon file in calibration file for the serial number of sensor
                if tag is not None:
                    tag_date = sensor_root.find('.//' + 'CalibrationDate')
                    calib_date = calib_element.find('CalibrationDate')
                    if float(tag.text) != float(calib.text):
                        buffer.write(f"Checking {calib.tag}: {calib.text}\n")
                        buffer.write(f"Check FAILED: calibration {calib.tag} and {calib.text} do not match values in calib file: {tag.text}\n")
                        record_failure(serial_number)
                    elif tag_date.text != calib_date.text:
                        buffer.write(f"Checking {calib.tag}: {calib.text}\n")
                        tag_date_str = check_date(tag_date.text)
                        calib_date_str = check_date(calib_date.text)
                        if (tag_date_str != calib_date_str) or (tag_date_str == '') or (calib_date_str == ''):
                            buffer.write(f"Check FAILED: calibration date {calib_date.text} does not match values in calib file: {tag_date.text}\n")
                            record_failure(serial_number)
                        else:
                            buffer.write(f"Checking {calib.tag}: {calib.text}\n")
                            buffer.write(f"Calibration Value and Date Checks passed\n")
                    else:
                        buffer.write(f"Checking {calib.tag}: {calib.text}\n")
                        buffer.write(f"Calibration Value and Date Checks passed\n")
                else:
                    buffer.write(f"Check FAILED: calibration {calib.tag} not found in calib file\n")    
                    record_failure(serial_number)
                    
    # Check Coefficients
    check_coefficients(xmlcon_root, sensor_element, sensor_root, serial_number) 

def find_calibration_elements(xmlcon_root, tag):
    """Find calibration elements matching a specific tag."""
    return xmlcon_root.findall('.//' + tag)

def find_element_by_serial(elements, serial_number):
    """Find an element by serial number."""
    for element in elements:
        serial = element.find('SerialNumber')
        if serial.text == serial_number:
            return element
    return None

def format_date(calib_date_text):
    """Format the date from YYYY/MM/DD to different string representations."""
    try:
        parsed_date = datetime.strptime(calib_date_text, "%m/%d/%Y")
        short_month = parsed_date.strftime("%b")
        full_month = parsed_date.strftime("%B")
        num_month = parsed_date.strftime("%m")
        day = str(int(parsed_date.strftime("%d")))
        year = parsed_date.strftime("%Y")
        short_year = parsed_date.strftime("%y")
        return {
            "mdy_dash": f"{day}-{short_month}-{short_year}",
            "mdy_comma": f"{full_month} {day}, {year}",
            "mdy_slash" : f"{num_month}/{day}/{year}",
            "mdy_slash2" : f"{num_month}/{day}/{short_year}"
        }
    except:
        return ""

def check_date_in_sensor_root(date_formats, sensor_root, serial_number):
    """Check if any formatted date exists in the sensor root."""
    for date_str in date_formats.values():
        if date_str in sensor_root:
            buffer.write(f"Date check passed\n")
            return True
    buffer.write(f"Date check failed, expecting one of {date_formats.values()}\n")
    record_failure(serial_number)
    return False

def check_calibration_values(calib_element, sensor_root, buffer, serial_number):
    """Check other calibration values excluding serial number and date."""
    for calib in calib_element:
        if calib.tag not in ['SerialNumber', 'CalibrationDate']:
            buffer.write(f"Checking {calib.tag}: {calib.text}\n")
            calib_value = format_calibration_value(calib.text)
            if calib_value != ValueError:
                if calib_value in sensor_root:
                    buffer.write(f"Value check passed\n")
                else:
                    buffer.write(f"Check FAILED: calibration {calib.tag} and {calib_value} not found in calib file\n")
                    record_failure(serial_number)
            else:
                buffer.write(f"Check FAILED: calibration {calib.tag} and {calib.text} not found in calib file\n")
                record_failure(serial_number)
                    
                for coef_calib in calib:     #check coefficients 
                    buffer.write(f"Checking {coef_calib.tag}: {coef_calib.text}\n")
                    if coef_calib.text in sensor_root:
                        buffer.write(f"Value check passed\n")
                    else:
                        buffer.write(f"Check FAILED: calibration {coef_calib.tag} and {coef_calib.text} not found in calib file\n")
                        record_failure(serial_number)

def format_calibration_value(value):
    """Format calibration value by removing trailing zeros."""
    try:
        value = float(value)
        value = str(value).rstrip('0')
        if value.endswith('.'):
            value = value[:-1]
    except ValueError:
        return ValueError
    return value

def check_pdf(xmlcon_root, sensor_element, sensor_root, serial_number):
    """Main function to check PDF against calibration records."""
    for element in sensor_element:
        buffer.write(f"{element.tag}: {element.text}\n")
        calib_elements = find_calibration_elements(xmlcon_root, element.tag)
        calib_element = find_element_by_serial(calib_elements, serial_number)
        if calib_element:
            calib_date = calib_element.find('CalibrationDate')
            if calib_date.text:
                formatted_date_str = check_date(calib_date.text)
                date_formats = format_date(formatted_date_str)
                if date_formats != "":
                    check_date_in_sensor_root(date_formats, sensor_root, serial_number)
                else:
                    buffer.write(f"Date check failed, expecting {calib_date.text}\n")   # some pdf files contain images which cannot be read
                    record_failure(serial_number)
            check_calibration_values(calib_element, sensor_root, buffer, serial_number)

def confirm_calibration_diff(xmlcon_file_path, diff_text, calib_file_path):
    buffer.write(f"\n---------->The following file is used to check the calibration values:<----------\n")
    buffer.write(f"Path: {xmlcon_file_path}\n")
    
    if 'Nmea' not in diff_text:    #confusing diff text has NMEA - AR61a, AR61b, AR78
        # read in the xmlcon file in the ctd directory
        xmlcon_root = get_data(xmlcon_file_path)
    
        # Find Sensor elements in xmlcon file
        sensor_elements = find_calibration_elements(xmlcon_root, 'Sensor')

        pattern = r'/Sensor\[(\d+)\]/'
        matches = re.findall(pattern, diff_text)
        unique_matches = set(matches)     #eliminate duplicate matches
        matches = list(unique_matches)
        for match in matches:
            sensor_index = int(match)
    
            # get the sensor at the sensor_index
            sensor_element = sensor_elements[sensor_index - 1]
            serial_number_element = sensor_element.find('.//SerialNumber')
            if serial_number_element is not None:
                serial_number = serial_number_element.text
                if serial_number is not None:
                    buffer.write(f"_____________________________________________________________________________________________________\n")
                
                    # Look for sensor serial number .xml file in the calibration directory
                    sensor_file = find_calib_file_with_serial_number(calib_file_path, serial_number)
                    if sensor_file:
                        buffer.write(f"Calibration file found: {sensor_file}\n")
                        buffer.write(f"Sensor SerialNumber: {serial_number}\n")
                        sensor_root = get_data(sensor_file)
                        if sensor_root:
                            if sensor_file.lower().endswith(".xml"):
                                check_calibrations(xmlcon_root, sensor_element, sensor_root, serial_number)
                            else:
                                check_pdf(xmlcon_root, sensor_element, sensor_root, serial_number)  
                    else:
                        buffer.write(f"No Calibration file found with serial number {serial_number}\n")   
                else:
                    buffer.write(f"Serial Number missing for sensor index: {sensor_index-1}\n")   
    else:
        buffer.write(f"Nmea change, no sensor changes\n")   

def confirm_calibration(xmlcon_file_path, calib_file_path):
    buffer.write(f"\n---------->The following file is used to check the calibration values:<----------\n")
    buffer.write(f"Path: {xmlcon_file_path}\n")
   
    # read in the xmlcon file in the ctd directory
    xmlcon_root = get_data(xmlcon_file_path)
   
    # Find all Sensor elements in xmlcon file
    sensor_elements = find_calibration_elements(xmlcon_root, 'Sensor')

    # Iterate through each Sensor element and extract the SerialNumber
    for sensor_element in sensor_elements:
        serial_number_element = sensor_element.find('.//SerialNumber')
        if serial_number_element is not None:
            serial_number = serial_number_element.text
            if serial_number is not None:
                buffer.write(f"_____________________________________________________________________________________________________\n")
                
                # Look for sensor serial number .xml file in the calibration directory
                sensor_file = find_calib_file_with_serial_number(calib_file_path, serial_number)
                if sensor_file:
                    buffer.write(f"Calibration file found: {sensor_file}\n")
                    buffer.write(f"Sensor SerialNumber: {serial_number}\n")
                    sensor_root = get_data(sensor_file)
                    if sensor_root:
                        if sensor_file.lower().endswith(".xml"):
                            check_calibrations(xmlcon_root, sensor_element, sensor_root, serial_number)
                        else:
                            check_pdf(xmlcon_root, sensor_element, sensor_root, serial_number)  
                else:
                    buffer.write(f"No Calibration file found with serial number {serial_number}\n")
 
                    
def compare_all_xmlcon(xmlcon_file_path):
    diff_files = []
    #'diffcheck' the .XMLCONs in the directory
    files = find_xmlcon_files(xmlcon_file_path)
    
    def compare_xmls(observed,expected):
        formatter = formatting.DiffFormatter()
        diff = xmldiff_main.diff_files(observed,expected,formatter=formatter)         
        return diff
    
    buffer.write(f"First XMLCON file: {files[0]}\n")
    buffer.write(f"Last XMLCON file: {files[len(files)-1]}\n")

    for i in range(len(files)):
        out = compare_xmls(files[0], files[i])
        if out != "":
            buffer.write(f"XMLCON file does not match: {files[i]}\n")
            diff_files.append((files[i], out))
    
    return files[0], diff_files

def check_btl_files(xmlcon_file_path):
    if '\\raw' in xmlcon_file_path:
        # btl files are located in /proc dir
        xmlcon_file_path = xmlcon_file_path.replace(r'\raw', r'\proc')
    # for every .hdr file, check if there's a btl file with the same name
    warning = False
    for hdr_file in glob(os.path.join(xmlcon_file_path, '*.hdr')):
        if (not os.path.basename(hdr_file).startswith(('d', 'u')) and 
            '_u.' not in os.path.basename(hdr_file) and
            '_up.' not in os.path.basename(hdr_file) and
            '_down.' not in os.path.basename(hdr_file)):
            btl_file = hdr_file[:-4] + '.btl'  # Replace the .hdr extension with .btl
            # Check if the .btl file exists
            if not os.path.exists(btl_file):
                buffer.write(f"WARNING: No corresponding .btl file found for {hdr_file}\n")
                warning = True
            
    if not warning:
        buffer.write(f"Verified a corresponding .btl file for each .hdr file in: {xmlcon_file_path}\n")
    buffer.write(f"\n")


def review_data(xmlcon_file_path, calib_file_path):
    match = re.search(r'ship-provided_data_(.*?)\\', xmlcon_file_path)
    if match:
        cruise_name = match.group(1)
    else:
        print(f"Cruise name pattern not found in file path.")
        buffer.write(f"Cruise name pattern not found in file path.<br>")
        
    if os.path.exists(xmlcon_file_path) and os.path.exists(calib_file_path):
        if "xmlcon" in xmlcon_file_path:
            confirm_calibration(xmlcon_file_path, calib_file_path)
        else:
            result_file, diff_files = compare_all_xmlcon(xmlcon_file_path)
            if not diff_files:
                buffer.write(f"All .XMLCON files match!!!!\n")
                buffer.write(f"\n")
            else:
                buffer.write(f"Reference README to see if there's a legitimate reason.\n")
                buffer.write(f"\n")
            check_btl_files(xmlcon_file_path)
            confirm_calibration(result_file, calib_file_path)  #check calibrations in the first xmlcon file
            if diff_files:
                buffer.write(f"\n---------------------------------------------------------------------------------------------------\n")
                buffer.write(f"---------------------------------------------------------------------------------------------------\n")
                buffer.write(f"\nChecking XMLCON File Differences.\n")
                buffer.write(f"\n---------------------------------------------------------------------------------------------------\n")
                buffer.write(f"---------------------------------------------------------------------------------------------------\n")
                buffer.write(f"\n")
                for file, diff in diff_files:
                    confirm_calibration_diff(file, diff, calib_file_path)  #check calibrations in the difference in other xmlcon files
    else:
        buffer.write(f"ERROR: Required directory does not exist: {xmlcon_file_path}, {calib_file_path}\n")
            
    summary.write(f"Summary Report\n")
    summary.write(f"List of Failed Instrument Serial Numbers: \n")
    summary.write(f"{failed_instruments}\n")
    summary.write(f"\n")
    summary_content = summary.getvalue()
    summary.close()
    with open("{}/{}_ctd_calibration_results.txt".format(current_dir, cruise_name), "w") as file:
        file.write(summary_content)
            
    buffer_content = buffer.getvalue()
    buffer.close()
    with open("{}/{}_ctd_calibration_results.txt".format(current_dir, cruise_name), "a") as file:
        file.write(buffer_content)
    

def main():
    # If paths are on Google Docs, you must download the files to your local pc file system.
    parser = argparse.ArgumentParser(description='Review CTD data prior to upload to RDS for NES-LTER REST API.')
    parser.add_argument('path', type=str, help='Path to ctd xmlcon directory or file')
    parser.add_argument('calib', type=str, help='Path to Calibration file directory')   # typically is ctd/doc dir
    
    args = parser.parse_args()       
    review_data(args.path, args.calib)

if __name__ == '__main__':
    main()
