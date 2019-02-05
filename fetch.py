import censusdata
import yaml
import os.path
import sys

if not os.path.isfile('config.yml'):
  print('First make a copy of config.yml.dist, named config.yml, and edit it as needed.')
  sys.exit()

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

# Use these defaults for all indicators.
defaults = {
  'survey': 'acs5',
  'years': [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017],
}

# The list of indicators to generate and calculations to perform.
indicators = {
  '5.1.1': {
    'variables': [
      'B14003_030E', # total female
      'B14003_031E', # female public school
      'B14003_040E', # female private school
    ],
    # (Public + Private) / Total
    'calculation': lambda row: (row['B14003_031E'] + row['B14003_040E']) / row['B14003_030E'] * 100,
  },
  '5.5.1': {
    'variables': [
      'C24010_041E', # female
      'C24010_005E', # male
    ],
    # Female / (Female + Male)
    'calculation': lambda row : row['C24010_041E'] / (row['C24010_041E'] + row['C24010_005E']) * 100,
  }
}

for id in indicators:
  indicator = defaults.copy()
  indicator.update(indicators[id])
  df = None
  for year in indicator['years']:
    data = censusdata.download(indicator['survey'], year, geo, indicator['variables'], key=config['api_key'])
    data['Year'] = year
    data['Value'] = data.apply(indicator['calculation'], axis=1)
    if df is None:
      df = data
    else:
      df = df.append(data)

  df = df.drop(indicators[id]['variables'], axis='columns')
  df = df.round(2)
  df.to_csv(id + '.csv', index=False)