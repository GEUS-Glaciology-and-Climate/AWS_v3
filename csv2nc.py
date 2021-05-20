import numpy as np
import pandas as pd
import xarray as xr
import datetime
import glob
import os

# load CSV to NetCDF lookup variable lookup table
v = pd.read_csv('./variables.csv', index_col=0)
vf = v.set_index('field')

# For each CSV file...
CSV_list = glob.glob('./out/*hour*')
nc_list = [_.split('/')[-1].split('.txt')[0][:-4][:-5]+'.nc' for _ in CSV_list]
for fname,ncname in zip(CSV_list, nc_list):
    print(f'Generating {ncname} from {fname}')
    df = pd.read_csv(fname,
                     delimiter="\s+",
                     parse_dates={'time':[0,1,2,3]},
                     infer_datetime_format=True,
                     date_parser=lambda y,m,d,h: datetime.datetime(int(y),int(m),int(d),int(h)),
                     index_col=0)
    
    df = df.replace(-999, np.nan)
    
    # Drop unused columns
    for c in df.columns:
        if c not in v.index: df.drop(columns=c, inplace=True)
    
    # Rename columns to CF standards
    df.columns = [v.loc[c]['field'] for c in df.columns]
    
    # Convert from pandas to xarray
    ds = xr.Dataset(df)
    
    # Add CF metdata
    for k in ds.keys():
        ds[k].attrs['standard_name'] = vf.loc[k]['standard_name']
        ds[k].attrs['long_name'] = vf.loc[k]['long_name']
        ds[k].attrs['units'] = vf.loc[k]['units']
    
    # Also add metadat for 'time' variable'
    ds['time'].attrs['standard_name'] = 'time'
    ds['time'].attrs['long_name'] = 'time'
    for k in ds.keys(): # for each var
            if ds[k].attrs['units'] == 'C':
                attrs = ds[k].attrs
                ds[k] = ds[k] - 273.15
                attrs['units'] = 'K'
                ds[k].attrs = attrs
    
    # Add ACDD metadata
    ds.attrs['id'] = 'AWS_v3'
    ds.attrs['naming_authority'] = 'dk.promice'
    ds.attrs['title'] = 'PROMICE AWS data'
    ds.attrs['summary'] = 'PROMICE AWS data'
    ds.attrs['keywords'] = 'PROMICE, AWS, Greenland, weather station'
    ds.attrs['geospatial_lat_min'] = ds['gps_lat'].min().values
    ds.attrs['geospatial_lat_max'] = ds['gps_lat'].max().values
    ds.attrs['geospatial_lon_min'] = ds['gps_lon'].min().values
    ds.attrs['geospatial_lon_max'] = ds['gps_lon'].max().values
    ds.attrs['time_coverage_start'] = str(ds['time'][0].values)
    ds.attrs['time_coverage_end'] = str(ds['time'][-1].values)
    ds.attrs['Conventions'] = 'ACDD-1.3, CF-1.7'
    ds.attrs['history'] = 'csv2.nc.py'
    ds.attrs['source'] = 'PROMICE AWS L3 processing scripts'
    ds.attrs['processing_level'] = 'Level 3 NetCDF conversion from CSV output'
    ds.attrs['date_created'] = str(datetime.datetime.now().isoformat())
    ds.attrs['creator_type'] = 'person'
    ds.attrs['creator_institution'] = 'GEUS'
    ds.attrs['creator_name'] = 'Ken Mankoff'
    ds.attrs['creator_email'] = 'kdm@geus'
    ds.attrs['creator_url'] = 'http://kenmankoff.com'
    ds.attrs['institution'] = 'GEUS'
    ds.attrs['publisher_name'] = 'Ken Mankoff'
    ds.attrs['publisher_email'] = 'kdm@geus.dk'
    ds.attrs['publisher_url'] = 'http://promice.dk'
    ds.attrs['project'] = 'PROMICE'
    ds.to_netcdf('./out/'+ncname)
