import os
import sys

import numpy as np
import pandas as pd
from urllib.error import HTTPError

import argparse

from neslter.parsing.utils import clean_column_names, wide_to_long

# read command line arguments
parser = argparse.ArgumentParser(description='Parse HPLC sample log and CTD metadata.')
parser.add_argument('-b', '--base_dir', required=True, help='Base directory path containing the data root')
parser.add_argument('-i', '--ims_host', required=True, help='IMS hostname')
args = parser.parse_args()

BASE_DIR = args.base_dir
IMS_HOST = args.ims_host

# Read and clean up sample log

# your paths may vary
DIR = os.path.join(BASE_DIR, 'ims_data_root', 'raw', 'all')

assert os.path.exists(DIR)

fname = 'LTER_sample_log.xlsx'
path = os.path.join(DIR, fname)
assert os.path.exists(path)

print('reading sample log from', path)

raw = pd.read_excel(path,na_values=['-'], dtype={
    'HPLC_a\nHSL #': str,
    'HPLC_b\nHSL #': str
})

df = clean_column_names(raw, {
    'Date (UTC)': 'date',
    'Start Time (UTC)': 'time',
    'Niskin #': 'niskin',
    'Niskin Target Depth': 'depth',
    'HPLC_a HSL #': 'hplca',
    'HPLC_b HSL #': 'hplcb'
})
df['Comments'] = df.comments.fillna('')
df = df[['cruise','cast','niskin','station_depth','hplca','hplc_a_vol','hplcb','hplc_b_vol']].dropna(subset=['hplca'])

mask = ~df.hplca.isna() & df.hplcb.isna()
df['is_replicate'] = 'D'
df.loc[mask, 'is_replicate'] = 'S'

start_sample_number = 1 # 1-based

hplclist = wide_to_long(df, [['hplca', 'hplc_a_vol'], ['hplcb', 'hplc_b_vol']],
            ['id_number', 'volume_filtered'],
            'replicate', ['a','b']).dropna(subset=['id_number'])
hplclist.to_csv('hplclist.csv')
hplclist['id_number'] = hplclist['id_number'].astype(int)
hplclist.sort_values('id_number', inplace=True)
hplclist['sequential_sample_number'] = range(start_sample_number,start_sample_number + len(hplclist))
hplclist['pi_sample_label'] = 'HSL_' + hplclist.id_number.astype(str)

def get_metadata(cruises):
    dfs = []
    for c in cruises:
        try:
            url = f'https://{IMS_HOST}/api/ctd/{c}/metadata.csv'
            dfs.append(pd.read_csv(url))
        except HTTPError:
            pass
    return pd.concat(dfs, sort=True)

ctd_md = get_metadata(hplclist.cruise.unique())

ctd_md.pop('latitude')
ctd_md.pop('longitude')

def hplc_format_dates(df):
    df = df.copy()
    timestamps = pd.to_datetime(df.date)
    yr = timestamps.dt.strftime('%Y').astype(int)
    mo = timestamps.dt.strftime('%b')
    dom = timestamps.dt.strftime('%d').astype(int)
    doy = timestamps.dt.strftime('%j').astype(int)
    hm = timestamps.dt.strftime('%H:%M') 
    df['year'] = yr
    df['month'] = mo
    df['day_of_month'] = dom
    df['day_of_year'] = doy
    df['time'] = hm
    return df

w_ctd_md = hplclist.merge(ctd_md, on=['cruise','cast'])

cruises = ctd_md.cruise.unique()
print('fetching bottle metadata for {} cruises'.format(len(cruises)))

def get_bottles(cruises):
    dfs = []
    for c in cruises:
        try:
            url = 'https://nes-lter-data.whoi.edu/api/ctd/{}/bottle_summary.csv'.format(c)
            print('fetching', url)
            dfs.append(pd.read_csv(url))
        except HTTPError:
            print('failed to fetch', url)
            pass
    return pd.concat(dfs, sort=True)

btls = get_bottles(cruises)[['cruise','cast','niskin','depth','date','latitude','longitude']]
everything = w_ctd_md.merge(btls, on=['cruise','cast','niskin'], how='left').sort_values('sequential_sample_number')
# now coalesce the date_x and date_y columns, filling missing values with the other
everything['date'] = everything['date_y'].fillna(everything['date_x'])
everything['depth'] = everything['depth'].map(lambda x: '{:.1f}'.format(x))
everything['latitude'] = everything['latitude'].map(lambda x: '{:.3f}'.format(x))
everything['longitude'] = everything['longitude'].map(lambda x: '{:.3f}'.format(x))
everything['nearest_station'] = 'closest LTER station:  ' + everything['nearest_station']

final = everything.copy()
final['pi'] = 'Sosik, Heidi'
final['body_of_water'] = 'North East Shelf'
final['water_type'] = 'Coastal'
final['vacuum'] = 'V'
final['filter_type'] = 'GF/F'
final['filter_diameter'] = 25
final['filter_storage'] = 'Liquid Nitrogen'

final = hplc_format_dates(final)

final = final.sort_values('date')

final = final[[
    'pi',
    'pi_sample_label',
    'cruise',
    'sequential_sample_number',
    'is_replicate',
    'volume_filtered',
    'cast',
    'niskin',
    'depth',
    'station_depth',
    'body_of_water',
    'water_type',
    'vacuum',
    'year',
    'month',
    'day_of_month',
    'day_of_year',
    'time',
    'longitude',
    'latitude',
    'filter_type',
    'filter_diameter',
    'filter_storage',
    'nearest_station'
]]
final.to_excel('hplc_coc.xlsx')
