This is an experimental package intended to make it easy to gather SDG data for a United States locality, such as a county or a city. The data is output in CSV files, in a format designed to be compatible with [Open SDG](https://github.com/open-sdg/open-sdg).

## Usage

1. Copy the `config.yml.dist` file to `config.yml`. Edit as needed.
1. Run `pipenv install`.
1. Run `pipenv run python fetch.py`.
1. See the generated CSV files.

## Roadmap

* Add more indicators
