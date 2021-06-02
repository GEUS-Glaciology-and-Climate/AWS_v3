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
    df = pd.read_csv(fname, delimiter="\s+")
    df['time'] = pd.to_datetime(df[['Year', 'MonthOfYear', 'DayOfMonth','HourOfDay(UTC)']]\
                              .rename(columns={'MonthOfYear':'month',
                                               'DayOfMonth':'day',
                                               'HourOfDay(UTC)':'hour'}))
    df = df.set_index('time').drop(columns=['Year','MonthOfYear','DayOfMonth','HourOfDay(UTC)',
                                            'DayOfYear', 'DayOfCentury'])
    
    df = df.replace(-999, np.nan)
    
    # Drop unused columns
    for c in df.columns:
        if c not in v.index: df.drop(columns=c, inplace=True)
    
    
    # Rename columns to CF standards
    df.columns = [v.loc[c]['field'] for c in df.columns]
    
    # Convert from pandas to xarray
    # ds = xr.Dataset(df)
    
    ds = xr.Dataset(coords=dict(time=df.index))
    
    for c in df.columns:
        ds[c] = df[c]
    
    
    ds.attrs["featureType"] = "timeSeries"
    
    # ds['time'].encoding['units'] = 'hours since 2016-05-01 00:00:00'
    # ds['time'] = ds['time'].astype('datetime64[D]')
    
    # Add CF metdata
    for k in ds.keys():
        ds[k].attrs['standard_name'] = vf.loc[k]['standard_name']
        ds[k].attrs['long_name'] = vf.loc[k]['long_name']
        ds[k].attrs['units'] = vf.loc[k]['units']
    
    
    # Also add metadat for 'time' variable'
    ds['time'].attrs['standard_name'] = 'time'
    ds['time'].attrs['long_name'] = 'time'
    
    # # ds['time'] = (ds['time'] - ds['time'][0]).dt.seconds.astype(np.int)
    # a = ds['time'].attrs
    # if 'units' in a: a.pop('units')
    # a['units'] = 'seconds since ' + ds['time'][0].values.astype(np.str)
    # ds['time'] = (ds['time'] - ds['time'][0]).dt.seconds.astype(np.int)
    # for kk,vvv in a.items():
    #     print(kk)
    #     print(vvv)
    #     ds['time'].attrs[kk] = str(vvv)
    # # ds['time'] = ds['time'].astype(np.int)
    
    a = ds['gps_lon'].attrs
    ds['gps_lon'] = -1 * ds['gps_lon']
    ds['gps_lon'].attrs = a
    ds['gps_lon'].attrs['units'] = 'degrees_east'
    
    ds['lon'] = ds['gps_lon'].mean()
    ds['lon'].attrs = a
    ds['lon'].attrs['units'] = 'degrees_east'
    
    ds['lat'] = ds['gps_lat'].mean()
    ds['lat'].attrs = ds['gps_lat'].attrs
    
    ds['alt'] = ds['gps_alt'].mean()
    ds['alt'].attrs = ds['gps_alt'].attrs
    ds['alt'].attrs['positive'] = 'up'
    ds['gps_alt'].attrs['positive'] = 'up'
    
    # ds = ds.drop(['gps_lon','gps_lat','gps_alt'])
    
    # ds['station_name'] = (('name_strlen'), [fname.split('hour')[0].split('/')[2][:-1]])
    # # ds['station_name'].attrs['long_name'] = 'station name'
    # ds['station_name'].attrs['cf_role'] = 'timeseries_id'
    
    ds['albedo_70'].attrs['units'] = '-'
    # for k in ds.keys(): # for each var
    #     if 'units' in ds[k].attrs:        
    #         if ds[k].attrs['units'] == 'C':
    #             attrs = ds[k].attrs
    #             ds[k] = ds[k] - 273.15
    #             attrs['units'] = 'K'
    #             ds[k].attrs = attrs
    for k in ds.keys(): # for each var
        if 'units' in ds[k].attrs:        
            if ds[k].attrs['units'] == 'C':
                ds[k].attrs['units'] = 'degrees_C'
    
    # https://wiki.esipfed.org/Attribute_Convention_for_Data_Discovery_1-3#geospatial_bounds
    
    # highly recommended
    ds.attrs['title'] = 'PROMICE AWS data'
    
    ds.attrs['summary'] = 'The Programme for Monitoring of the Greenland Ice Sheet (PROMICE) has been measuring climate and ice sheet properties since 2007. Currently the PROMICE automatic weather station network includes 25 instrumented sites in Greenland. Accurate measurements of the surface and near-surface atmospheric conditions in a changing climate is important for reliable present and future assessment of changes to the Greenland ice sheet. Here we present the PROMICE vision, methodology, and each link in the production chain for obtaining and sharing quality-checked data. In this paper we mainly focus on the critical components for calculating the surface energy balance and surface mass balance. A user-contributable dynamic webbased database of known data quality issues is associated with the data products at (https://github.com/GEUS-PROMICE/ PROMICE-AWS-data-issues/). As part of the living data option, the datasets presented and described here are available at DOI: 10.22008/promice/data/aws, https://doi.org/10.22008/promice/data/aws'
    
    ds.attrs['keywords'] = ['GCMDSK:EARTH SCIENCE > CRYOSPHERE > GLACIERS/ICE SHEETS > ICE SHEETS > ICE SHEET MEASUREMENTS',
     'GCMDSK:EARTH SCIENCE > CRYOSPHERE > GLACIERS/ICE SHEETS > GLACIER MASS BALANCE/ICE SHEET MASS BALANCE',
     'GCMDSK:EARTH SCIENCE > CRYOSPHERE > SNOW/ICE > SNOW/ICE TEMPERATURE',
     'GCMDSK:EARTH SCIENCE > CRYOSPHERE > SNOW/ICE',
     'GCMDSK:EARTH SCIENCE > CRYOSPHERE > SNOW/ICE > SNOW MELT',
     'GCMDSK:EARTH SCIENCE > CRYOSPHERE > SNOW/ICE > SNOW DEPTH',
     'GCMDSK:EARTH SCIENCE > CRYOSPHERE > SNOW/ICE > ICE VELOCITY',
     'GCMDSK:EARTH SCIENCE > CRYOSPHERE > SNOW/ICE > ALBEDO',
     'GCMDSK:EARTH SCIENCE > TERRESTRIAL HYDROSPHERE > SNOW/ICE > ALBEDO',
     'GCMDSK:EARTH SCIENCE > TERRESTRIAL HYDROSPHERE > SNOW/ICE > ICE GROWTH/MELT',
     'GCMDSK:EARTH SCIENCE > TERRESTRIAL HYDROSPHERE > SNOW/ICE > ICE VELOCITY',
     'GCMDSK:EARTH SCIENCE > TERRESTRIAL HYDROSPHERE > SNOW/ICE > SNOW DEPTH',
     'GCMDSK:EARTH SCIENCE > TERRESTRIAL HYDROSPHERE > SNOW/ICE > SNOW MELT',
     'GCMDSK:EARTH SCIENCE > TERRESTRIAL HYDROSPHERE > SNOW/ICE > SNOW/ICE TEMPERATURE',
     'GCMDSK:EARTH SCIENCE > TERRESTRIAL HYDROSPHERE > SNOW/ICE',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC PRESSURE',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > ALBEDO',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > INCOMING SOLAR RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > LONGWAVE RADIATION > DOWNWELLING LONGWAVE RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > LONGWAVE RADIATION > UPWELLING LONGWAVE RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > LONGWAVE RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > NET RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > OUTGOING LONGWAVE RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > RADIATIVE FLUX',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > RADIATIVE FORCING',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > SHORTWAVE RADIATION > DOWNWELLING SHORTWAVE RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > SHORTWAVE RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > SUNSHINE',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC TEMPERATURE > SURFACE TEMPERATURE > AIR TEMPERATURE',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WATER VAPOR > WATER VAPOR INDICATORS > HUMIDITY > ABSOLUTE HUMIDITY',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WATER VAPOR > WATER VAPOR INDICATORS > HUMIDITY > RELATIVE HUMIDITY',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > LOCAL WINDS',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS > U/V WIND COMPONENTS',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS > WIND DIRECTION',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS > WIND SPEED',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > CLOUDS',
     'GCMDSK:EARTH SCIENCE > ATMOSPHERE > PRECIPITATION']
    
    ds.attrs['Conventions'] = 'ACDD-1.3, CF-1.7'
    
    # recommended
    import uuid
    ds.attrs['id'] = 'dk.geus.promice:' + str(uuid.uuid4())
    ds.attrs['naming_authority'] = 'dk.geus.promice'
    ds.attrs['history'] = 'csv2.nc.py'
    ds.attrs['source'] = 'PROMICE AWS L3 processing scripts'
    ds.attrs['processing_level'] = 'Level 3 NetCDF conversion from CSV output'
    ds.attrs['acknowledgement'] = 'The Programme for Monitoring of the Greenland Ice Sheet (PROMICE)'
    ds.attrs['license'] = 'Freely distributed'
    ds.attrs['standard_name_vocabulary'] = 'CF Standard Name Table (v77, 19 January 2021)'
    ds.attrs['date_created'] = str(datetime.datetime.now().isoformat())
    ds.attrs['creator_name'] = 'Ken Mankoff'
    ds.attrs['creator_email'] = 'kdm@geus'
    ds.attrs['creator_url'] = 'http://kenmankoff.com'
    ds.attrs['institution'] = 'GEUS'
    ds.attrs['publisher_name'] = 'GEUS'
    ds.attrs['publisher_email'] = 'info@promice.dk'
    ds.attrs['publisher_url'] = 'http://promice.dk'
    
    ds.attrs['geospatial_bounds'] = "POLYGON((" + \
        f"{ds['lat'].min().values} {ds['lon'].min().values}, " + \
        f"{ds['lat'].min().values} {ds['lon'].max().values}, " + \
        f"{ds['lat'].max().values} {ds['lon'].max().values}, " + \
        f"{ds['lat'].max().values} {ds['lon'].min().values}, " + \
        f"{ds['lat'].min().values} {ds['lon'].min().values}))"
    ds.attrs['geospatial_bounds_crs'] = 'EPSG:4326'
    ds.attrs['geospatial_bounds_vertical_crs'] = 'EPSG:5829 CHECKME'
    ds.attrs['geospatial_lat_min'] = ds['lat'].min().values
    ds.attrs['geospatial_lat_max'] = ds['lat'].max().values
    ds.attrs['geospatial_lon_min'] = ds['lon'].min().values
    ds.attrs['geospatial_lon_max'] = ds['lon'].max().values
    ds.attrs['geospatial_vertical_min'] = ds['alt'].min().values
    ds.attrs['geospatial_vertical_max'] = ds['alt'].max().values
    ds.attrs['geospatial_vertical_positive'] = 'up'
    ds.attrs['time_coverage_start'] = str(ds['time'][0].values)
    ds.attrs['time_coverage_end'] = str(ds['time'][-1].values)
    # https://www.digi.com/resources/documentation/digidocs/90001437-13/reference/r_iso_8601_duration_format.htm
    ds.attrs['time_coverage_duration'] = pd.Timedelta((ds['time'][-1] - ds['time'][0]).values).isoformat()
    ds.attrs['time_coverage_resolution'] = pd.Timedelta((ds['time'][1] - ds['time'][0]).values).isoformat()
    
    # suggested
    ds.attrs['creator_type'] = 'person'
    ds.attrs['creator_institution'] = 'GEUS'
    ds.attrs['publisher_type'] = 'institution'
    ds.attrs['publisher_institution'] = 'GEUS'
    ds.attrs['program'] = 'PROMICE'
    ds.attrs['contributor_name'] = ''
    ds.attrs['contributor_role'] = ''
    ds.attrs['geospatial_lat_units'] = 'degrees_north'
    # ds.attrs['geospatial_lat_resolution'] = ''
    ds.attrs['geospatial_lon_units'] = 'degrees_east'
    # ds.attrs['geospatial_lon_resolution'] = ''
    ds.attrs['geospatial_vertical_units'] = 'EPSG:4979 CHECKME'
    # ds.attrs['geospatial_vertical_resolution'] = ''
    # ds.attrs['date_modified'] = ds.attrs['date_created']
    # ds.attrs['date_issued'] = ds.attrs['date_created']
    # ds.attrs['date_metadata_modified'] = ''
    ds.attrs['product_version'] = 3
    ds.attrs['keywords_vocabulary'] = 'GCMDSK:GCMD Science Keywords:https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/sciencekeywords, CFSTDN:NetCDF COARDS Climate and Forecast Standard Names'
    # ds.attrs['platform'] = ''
    # ds.attrs['platform_vocabulary'] = 'GCMD:GCMD Keywords'
    ds.attrs['instrument'] = 'See https://doi.org/10.5194/essd-2021-80'
    # ds.attrs['instrument_vocabulary'] = 'GCMD:GCMD Keywords'
    # ds.attrs['cdm_data_type'] = ''
    # ds.attrs['metadata_link'] = ''
    ds.attrs['references'] = 'https://doi.org/10.5194/essd-2021-80'
    
    
    ds.attrs['comment'] = 'N/A'
    
    # ds.attrs['geospatial_lat_extents_match'] = 'gps_lat'
    # ds.attrs['geospatial_lon_extents_match'] = 'gps_lon'
    
    
    # from shapely.geometry import Polygon
    # geom = Polygon(zip(ds['lat'].values, ds['lon'].values))
    # # print(geom.bounds)
    # ds.attrs['geospatial_bounds'] = geom.bounds
    
    
    
    
    
    
    
    ds.attrs['project'] = 'PROMICE'
    
    
    for vv in ['p', 't_1', 't_2', 'rh', 'sh', 'wspd', 'wdir', 'z_boom', 'z_stake', 'z_pt',
               't_i_1', 't_i_2', 't_i_3', 't_i_4', 't_i_5', 't_i_6', 't_i_7', 't_i_8',
               'tilt_x', 'tilt_y', 't_log']:
        ds[vv].attrs['coverage_content_type'] = 'physicalMeasurement'
        ds[vv].attrs['coordinates'] = "time lat lon alt"
    
    for vv in ['dshf', 'dlhf', 'dsr', 'dsr_cor', 'usr', 'usr_cor', 'albedo_70', 'dlr', 'ulr', 'cc', 't_surf', 'z_pt_cor']:
        ds[vv].attrs['coverage_content_type'] = 'modelResult'
        ds[vv].attrs['coordinates'] = "time lat lon alt"
    
    for vv in ['fan_dc', 'batt_v']:
        ds[vv].attrs['coverage_content_type'] = 'auxiliaryInformation'
        ds[vv].attrs['coordinates'] = "time lat lon alt"
    
    for vv in ['gps_hdop']:
        ds[vv].attrs['coverage_content_type'] = 'qualityInformation'
        ds[vv].attrs['coordinates'] = "time lat lon alt"
    
    for vv in ['gps_time', 'lon', 'lat', 'alt']:
        ds[vv].attrs['coverage_content_type'] = 'coordinate'
    
    
    ds['lon'].attrs['long_name'] = 'station longitude'
    ds['lat'].attrs['long_name'] = 'station latitude'
    ds['alt'].attrs['long_name'] = 'station altitude'
    
    ds['lon'].attrs['axis'] = 'X'
    ds['lat'].attrs['axis'] = 'Y'
    ds['alt'].attrs['axis'] = 'Z'
    
    for vv in ['lon', 'lat', 'alt']:
        ds[vv].attrs['coverage_content_type'] = 'coordinate'
    
    # for vv in []: ds[vv].attrs['coverage_content_type'] = 'referenceInformation'
    
    ds.time.encoding["dtype"] = "int32" # CF standard requires time as int not int64
    ds.to_netcdf('./out/'+ncname)
