## GBIF database creation and cleaning for GSDMS
Authors: Usha Nattala, Payal Bal

Please use the following steps to set up the gbif database.

1. Create GBIF database using master_create_gbifdb.R. This script offers the option to run python scipts either from the terminnal ir through R. It  includes the following steps: 
    
  a. Activates python environment
      source /home/ubuntu/environments/gsdms_env/bin/activate

  b. Download the data in raw csv from gbif.org (the steps will work for both full count csv as well as selected row csv)
      citation: https://doi.org/10.15468/dl.e3jr2r
      download link: http://api.gbif.org/v1/occurrence/download/request/0020324-191105090559680.zip
      python downloadcsv.py

  c. Make staging data
      python staging.py

  d. Make final data, clean and set up
      python make_db.py

  e. Delete script separated for safety
      python delete_lows.py
  
  f. Create summary tables from cleaned data
      python make_summary_tables.py
    
2. Delete the csv under /data/csv if no longer required

3. Clean GBIF backbone taxonomy and set it up as a database for referencing
    make_taxonomy_db.R

6. Create subset GBIF tables, e.g. for different classes: aves, reptilia, mammalia
    make_subset_table.R

7. Clean subset GBIF tables
    clean_subset_tables.R
    
8. Make PostGIS tables for global regionalisation schemes
    make_postgis_tables.R


Enjoy!



