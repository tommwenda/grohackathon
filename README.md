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
`>>$Counting records...- Found 78733 records` <br>
`>>$ Records found (78733) are more than 50000 please enter a filter.`
`>>$  eg commodity_desc=CORN`
`>>$  See http://quickstats.nass.usda.gov/api#param_define for more`
`>>$ Current query(You can replace the values too):`
`>>$ &sector_desc=CROPS&agg_level_desc=COUNTY&year__GE=2014&year__LE=2016`
`>>$ Enter filter: commodity_desc=POTATOES`

Note there are no quotes around the value POTATOES
