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

def find_calib_file_with_serial_number(url, serial_number):
    # url can be https or a local directory path
    try:
        response = requests.get(url)

        if response.status_code == 200:
           html_content = response.text
           soup = BeautifulSoup(html_content, 'html.parser')

           # Extracting file names from directory listing
           file_names = [a['href'] for a in soup.find_all('a') if a.has_attr('href')]
    except:
        try: 
            file_names = os.listdir(url)
        except:
            buffer.write(f"Directory does not exist: {url}\n")
            return None

    index = serial_number.find('s/n')
    if index != -1:
      serial_number = serial_number[index + 4:].strip()

    matching_files = []
    new_file_names = file_names
    
    # if file_name is a directory name
    for file_name in file_names:
        new_path = os.path.join(url, file_name)
        if os.path.isdir(new_path) and str(serial_number) in file_name:
            os.chdir(new_path)
            url = new_path
            new_file_names = os.listdir(new_path)       

    for file_name in new_file_names:
        if file_name.lower().endswith(".xml") and str(serial_number) in file_name:
            # there can be more than one file name that matches
            matching_files.append(file_name)
            
    if not matching_files:
        for file_name in new_file_names:
            if file_name.lower().endswith(".pdf") and str(serial_number) in file_name and 'Repair' not in file_name:
                # there can be more than one file name that matches
                matching_files.append(file_name)

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

def check_coefficients(xmlcon_root, sensor_element, sensor_root, serial_number ):
    # Loop through each sensor and process the Coefficients - need to get the tags under the Coefficient tag and verify those tags
    for element in sensor_element:
        buffer.write(f"{element.tag}: {element.text}\n")
        coef_index = 0         # coefficient tag equation index
        
        calib_elements = xmlcon_root.findall('.//' + element.tag)
        for element in calib_elements:
           calib_serial = element.find('SerialNumber')
           if serial_number == calib_serial.text:
               calib_element = element
               print(f"serial number: {serial_number}")

        # for each tag in sensor
        for calib in calib_element:                        
            if calib.tag == 'Coefficients' or calib.tag == 'CalibrationCoefficients':
                buffer.write(f"Checking {calib.tag}: {calib.attrib}\n")
                tag_occurances = sensor_root.findall('.//' + calib.tag)
                tag = tag_occurances[coef_index]
                # check if <Coefficients equation=> matches calibration xml file
                if tag is not None:
                    if tag.attrib != calib.attrib:
                        buffer.write(f"Check FAILED: calibration {calib.tag} and {calib.attrib} do not match values in calib file: {tag.attrib}\n")
                        if serial_number not in failed_instruments:
                            failed_instruments.append(serial_number)
                    else:
                        buffer.write(f"Calibration Value check passed\n")
                            
                    # loop thru coeficient tags in the equation <Coefficients equation="0"> and <Coefficients equation="1">
                    coef_elements = calib.findall('.//')
                    for coef in coef_elements: 
                        buffer.write(f"Checking {coef.tag}: {coef.text}\n")
                        ctags = sensor_root.findall('.//' + coef.tag)
                        # some ctags have multiples, but others do not
                        if len(ctags) > 1:
                            ctag = ctags[coef_index]  # index refers to equation 0 or 1 occurance
                        else:
                            ctag = ctags[0]
                        print(f"len(ctags) {len(ctags)}")
                        print(f"ctag {ctag}")
                        print(f"coef_index {coef_index}")
                        if ctag is not None:
                            if float(coef.text) != float(ctag.text):
                                buffer.write(f"Check FAILED: calibration {coef.tag} and {coef.text} do not match values in calib file: {ctag.text}\n")
                                if serial_number not in failed_instruments:
                                    failed_instruments.append(serial_number)
                            else:
                                buffer.write(f"Calibration Value check passed\n")

                        else:
                            buffer.write(f"Check FAILED: calibration {coef.tag} not found in calib file\n") 
                            if serial_number not in failed_instruments:
                                failed_instruments.append(serial_number)
                else:
                    buffer.write(f"Check FAILED: calibration {calib.tag} not found in calib file\n")   
                    if serial_number not in failed_instruments:
                        failed_instruments.append(serial_number)
                    
                coef_index = coef_index + 1

def check_calibrations(xmlcon_root, sensor_element, sensor_root, serial_number ):
    # For each element in the sensor element (TemperatureSensor, ConductivitySensor, PressureSensor, etc)
    for element in sensor_element:
        buffer.write(f"{element.tag}: {element.text}\n")
        calib_elements = xmlcon_root.findall('.//' + element.tag)
        for element in calib_elements:
           calib_serial = element.find('SerialNumber')
           if serial_number == calib_serial.text:
               calib_element = element

        # for each calibration tag in sensor
        for calib in calib_element:    
            # Process Coefficients separately below
            if calib.tag != 'SerialNumber' and calib.tag != 'CalibrationDate' and calib.tag != 'Coefficients' and calib.tag != 'CalibrationCoefficients':
                buffer.write(f"Checking {calib.tag}: {calib.text}\n")
                tag = sensor_root.find('.//' + calib.tag)
                # check if calibration tag and text in xmlcon file in calibration file for the serial number of sensor
                if tag is not None:
                    tag_date = sensor_root.find('.//' + 'CalibrationDate')
                    calib_date = calib_element.find('CalibrationDate')
                    if float(tag.text) != float(calib.text):
                        buffer.write(f"Check FAILED: calibration {calib.tag} and {calib.text} do not match values in calib file: {tag.text}\n")
                        if serial_number not in failed_instruments:
                            failed_instruments.append(serial_number)
                    elif tag_date.text != calib_date.text:
                        buffer.write(f"Check FAILED: calibration date {calib_date.text} does not match values in calib file: {tag_date.text}\n")
                        if serial_number not in failed_instruments:
                            failed_instruments.append(serial_number)
                    else:
                        buffer.write(f"Calibration Value and Date Checks passed\n")
                else:
                    buffer.write(f"Check FAILED: calibration {calib.tag} not found in calib file\n")    
                    if serial_number not in failed_instruments:
                        failed_instruments.append(serial_number)
                    
    # Check Coefficients
    check_coefficients(xmlcon_root, sensor_element, sensor_root, serial_number)
    
def check_pdf(xmlcon_root, sensor_element, sensor_root, serial_number ):
    for element in sensor_element:
        date_error = False
        buffer.write(f"{element.tag}: {element.text}\n")
        calib_elements = xmlcon_root.findall('.//' + element.tag)
        for element in calib_elements:
           calib_serial = element.find('SerialNumber')
           if serial_number == calib_serial.text:
               calib_element = element
               
        # check the date
        calib_date = calib_element.find('CalibrationDate')
        try:
            parsed_date = datetime.strptime(calib_date.text, "%d-%b-%y")
        except:
            try:
                parsed_date = datetime.strptime(calib_date.text, "%m/%d/%Y")
            except:
                try:
                    parsed_date = datetime.strptime(calib_date.text, "%Y%m%d")
                except:
                    buffer.write(f"Date check failed, invalid date string: {calib_date.text}\n")
                    date_error = True
                
        if not date_error:
            # remove leading zero from month and day
            month = str(int(parsed_date.strftime("%m")))
            day = str(int(parsed_date.strftime("%d")))  
            year = parsed_date.strftime("%Y")        
            # Format the parsed date as "MM/DD/YY"
            formatted_date_str = f"{month}/{day}/{year}"
                    
            if formatted_date_str in sensor_root:
                buffer.write(f"Date check passed\n")
            else:
                formatted_date_str = parsed_date.strftime("%B %d, %Y")
                if formatted_date_str in sensor_root:
                    buffer.write(f"Date check passed\n")
                elif "Date" not in sensor_root:
                    buffer.write(f"Date not found - some pdf files contain images which cannot be read\n")
                else:
                    buffer.write(f"Date check failed, expecting {formatted_date_str}\n")   # some pdf files contain images which cannot be read
                    if serial_number not in failed_instruments:
                        failed_instruments.append(serial_number)
               
        # for each calibration tag in sensor
        for calib in calib_element:    
            if calib.tag != 'SerialNumber' and calib.tag != 'CalibrationDate' and calib.tag != 'Coefficients' and calib.tag != 'CalibrationCoefficients':
                buffer.write(f"Checking {calib.tag}: {calib.text}\n")
                
                # Remove extra zeros after decimal point since the pdf files don't contain that granularity
                try:
                    calib.text = float(calib.text)
                except:
                    buffer.write(f"Check FAILED: calibration {calib.tag} invalid number: {calib.text} \n")  #AR34b
                    if serial_number not in failed_instruments:
                        failed_instruments.append(serial_number)
                        return
                calib.text = str(calib.text).rstrip('0')    
                # Remove the trailing dot if it exists
                if calib.text.endswith('.'):
                    calib.text = calib.text[:-1]
                
                if calib.text in sensor_root:
                    buffer.write(f"Value check passed\n")
                else:
                    buffer.write(f"Check FAILED: calibration {calib.tag} and {calib.text} not found in calib file\n")
                    if serial_number not in failed_instruments:
                        failed_instruments.append(serial_number)


def confirm_calibration(xmlcon_file_path, calib_file_path):
    buffer.write(f"The following file is used to check the calibration values:\n")
    buffer.write(f"Path: {xmlcon_file_path}\n")
    # get the ctd/doc dir for this cruise
    #doc_dir = get_doc_dir(xmlcon_file_path)
   
    # read in the xmlcon file in the ctd directory
    xmlcon_root = get_data(xmlcon_file_path)
   
    # Find all Sensor elements in xmlcon file
    sensor_elements = xmlcon_root.findall('.//Sensor')

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
    #'diffcheck' the .XMLCONs in the directory
    files = find_xmlcon_files(xmlcon_file_path)
    
    def compare_xmls(observed,expected):
        formatter = formatting.DiffFormatter()
        diff = xmldiff_main.diff_files(observed,expected,formatter=formatter)       
        return diff

    buffer.write(f"First XMLCON file: {files[0]}\n")
    buffer.write(f"Last XMLCON file: {files[len(files)-1]}\n")

    error_found = False    
    for i in range(len(files)):
        out = compare_xmls(files[0], files[i])
        if out != "":
            buffer.write(f"XMLCON file does not match: {files[i]}\n")
            error_found = True
    
    if error_found:
        return False
    else:
        return files[0]

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
            result_file = compare_all_xmlcon(xmlcon_file_path)
            if result_file:
                buffer.write(f"All .XMLCON files match!!!!\n")
                buffer.write(f"\n")
                check_btl_files(xmlcon_file_path)
                confirm_calibration(result_file, calib_file_path)
            else:
                buffer.write(f"Reference README to see if there's a legitimate reason.\n")
                buffer.write(f"\n")
                check_btl_files(xmlcon_file_path)
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
