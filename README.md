Authors: Usha Nattala, Payal Bal

Please use the following steps to set up the gbif database.

1. Activate python environment
    source ~/environments/gsdms_env/bin/activate

2. Now run the following scripts in /home/ubuntu/scripts/dataload:

  2.1 Download the data in raw csv from gbif.org (the steps will work for both full count csv as well as selected row csv)
  citation: https://doi.org/10.15468/dl.e3jr2r
  download link: http://api.gbif.org/v1/occurrence/download/request/0020324-191105090559680.zip
     python downloadcsv.py

  2.2 Make staging data
     python staging.py

  2.3 Make final data, clean and set up
     python make_db.py

  2.4 Delete script separated for safety
     python delete_lows.py

3. Delete the csv under /data/csv if no longer required

4. Enjoy!
