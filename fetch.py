import censusdata
import yaml
import os.path
import sys

# Use these defaults for all indicators.
defaults = {
  'survey': 'acs5',
  'years': [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017],
}

# The list of indicators to generate and calculations to perform.
indicators = {
  '5-1-1': {
    'variables': {
      'total': 'B14003_030E',
      'public': 'B14003_031E',
      'private': 'B14003_040E',
    },
    'calculation': lambda x: (x['public'] + x['private']) / x['total'] * 100,
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
    # Calculate the values.
    data['Value'] = data.apply(indicator['calculation'], axis=1)
    # Save the row.
    if df is None:
      df = data
    else:
      df = df.append(data)
  # Drop the variable columns since we don't need to export them.
  df = df.drop(indicator['variables'].keys(), axis='columns')
  # Round potentially long floats.
  df = df.round(2)
  # Export the CSV file.
  df.to_csv('indicator_' + id + '.csv', index=False)