# grohackathon Harvest script
Harvest data from United States Department of Agriculture, National Agricultural Service api http://quickstats.nass.usda.gov/api

## Requirements
psycopg2 - pip install psycopg2
requests - pip install requests

## Usage
From command line run python harvest.py
By default the script will fetch all data.
If data exceeds 50,000 records, the script will ask you to add some filters
For example after runnig the command python harvest.py:
`Enter filter: commodity_desc=POTATOES`

Note there are no quotes around the value POTATOES
