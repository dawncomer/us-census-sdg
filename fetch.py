import censusdata
import yaml
import os.path
import sys

# Use these defaults for all indicators.
defaults = {
  'survey': 'acs5',
  'years': [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017],
  'disaggregations': {},
}

# The list of indicators to generate and calculations to perform.
indicators = {
  '5-1-1': {
    'variables': {
      'total': 'B01001_026E',
      'public': 'B14003_031E',
      'private': 'B14003_040E',
    },
    'calculation': lambda x : (x['public'] + x['private']) / x['total'] * 100,
    'disaggregations': {
      'Age group': {
        '3 and 4 years': {
          'variables': {
            'total': 'B01001_027E', # Not ideal... this is "Under 5 years"
            'public': 'B14003_032E',
            'private': 'B14003_041E',
          },
        },
        '5 to 9 years': {
          'variables': {
            'total': 'B01001_028E',
            'public': 'B14003_033E',
            'private': 'B14003_042E',
          },
        },
        '10 to 14 years': {
          'variables': {
            'total': 'B01001_029E',
            'public': 'B14003_034E',
            'private': 'B14003_043E',
          },
        },
        '15 to 17 years': {
          'variables': {
            'total': 'B01001_030E',
            'public': 'B14003_035E',
            'private': 'B14003_044E',
          },
        },
        '18 and 19 years': {
          'variables': {
            'total': 'B01001_031E',
            'public': 'B14003_036E',
            'private': 'B14003_045E',
          },
        },
        '20 to 24 years': {
          'variables': {
            # We have to add together several variables to get this age group.
            't1': 'B01001_032E',
            't2': 'B01001_033E',
            't3': 'B01001_034E',
            'public': 'B14003_037E',
            'private': 'B14003_046E',
          },
          'calculation': lambda x : (x['public'] + x['private']) / (x['t1'] + x['t2'] + x['t3']) * 100,
        },
        '25 to 34 years': {
          'variables': {
            # We have to add together a couple of variables to get this age group.
            't1': 'B01001_035E',
            't2': 'B01001_036E',
            'public': 'B14003_038E',
            'private': 'B14003_047E',
          },
          'calculation': lambda x : (x['public'] + x['private']) / (x['t1'] + x['t2']) * 100,
        },
        '35 years and over': {
          'variables': {
            # We have to add together MANY variables to get this age group.
            't1': 'B01001_037E',
            't2': 'B01001_038E',
            't3': 'B01001_039E',
            't4': 'B01001_040E',
            't5': 'B01001_041E',
            't6': 'B01001_042E',
            't7': 'B01001_043E',
            't8': 'B01001_044E',
            't9': 'B01001_045E',
            't10': 'B01001_046E',
            't11': 'B01001_047E',
            't12': 'B01001_048E',
            't13': 'B01001_049E',
            'public': 'B14003_039E',
            'private': 'B14003_048E',
          },
          'calculation': lambda x : (x['public'] + x['private']) / (x['t1'] + x['t2'] + x['t3'] + x['t4'] + x['t5'] + x['t6'] + x['t7'] + x['t8'] + x['t9'] + x['t10'] + x['t11'] + x['t12'] + x['t13']) * 100,
        },
      },
    },
  },
  '5-5-1': {
    'variables': {
      'female': 'C24010_041E',
      'male': 'C24010_005E',
    },
    'calculation': lambda x : x['female'] / (x['female'] + x['male']) * 100,
  }
}

# Abort if config file is not present.
if not os.path.isfile('config.yml'):
  print('First make a copy of config.yml.dist, named config.yml, and edit it as needed.')
  sys.exit()

# Load the config settings.
with open('config.yml', 'r') as stream:
  try:
    config = yaml.load(stream)
  except yaml.YAMLError as exc:
    print(exc)

# Construct the geography for the API call.
geography_parts = []
for geo_id in config['geography']:
  geography_parts.append((geo_id, config['geography'][geo_id]))
geo = censusdata.censusgeo(geography_parts)

# Loop through the indicators to generate CSV files.
for id in indicators:
  # For each indicator, inherit from the 'defaults' dict.
  indicator = defaults.copy()
  indicator.update(indicators[id])
  df = None
  # We'll make separate API calls for each year.
  for year in indicator['years']:
    # Query the Census API.
    data = censusdata.download(indicator['survey'], year, geo, list(indicator['variables'].values()), key=config['api_key'])
    # Set the year.
    data['Year'] = year
    # Rename the variable columns so that the lambda functions will work.
    column_map = {indicator['variables'][k] : k for k in indicator['variables']}
    data = data.rename(column_map, axis='columns')
    # Calculate the aggregate values.
    data['Value'] = data.apply(indicator['calculation'], axis=1)
    # Save the row.
    if df is None:
      df = data
    else:
      df = df.append(data, sort=False)
  # Similar process for all disaggregations.
  for column in indicator['disaggregations']:
    for category in indicator['disaggregations'][column]:
      for year in indicator['years']:
        data = censusdata.download(indicator['survey'], year, geo, list(indicator['disaggregations'][column][category]['variables'].values()), key=config['api_key'])
        data['Year'] = year
        column_map = {indicator['disaggregations'][column][category]['variables'][k] : k for k in indicator['disaggregations'][column][category]['variables']}
        data = data.rename(column_map, axis='columns')
        if 'calculation' in indicator['disaggregations'][column][category]:
          data['Value'] = data.apply(indicator['disaggregations'][column][category]['calculation'], axis=1)
        else:
          data['Value'] = data.apply(indicator['calculation'], axis=1)
        data[column] = category
        data = data.drop(indicator['disaggregations'][column][category]['variables'].keys(), axis='columns')
        df = df.append(data, sort=False)
  # Drop the variable columns since we don't need to export them.
  df = df.drop(indicator['variables'].keys(), axis='columns')
  # Round potentially long floats.
  df = df.round(2)
  # Make sure "Year" is first and "Value" is last.
  cols = df.columns.tolist()
  cols.remove('Year')
  cols.insert(0, 'Year')
  cols.remove('Value')
  cols.append('Value')
  df = df[cols]

  # Export the CSV file.
  filename = 'indicator_' + id + '.csv'
  df.to_csv(filename, index=False)
  print('Successfully generated: ' + filename)