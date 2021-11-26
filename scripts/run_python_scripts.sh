source /home/ubuntu/environments/gsdms_env/bin/activate

## Download the data in raw csv from gbif.org (the steps will work for both full count csv as well as selected row csv)
## citation: https://doi.org/10.15468/dl.e3jr2r
## download link: http://api.gbif.org/v1/occurrence/download/request/0020324-191105090559680.zip
python /tempdata/workdir/gbifprocessing/scripts/downloadcsv.py

## Make staging data
python /tempdata/workdir/gbifprocessing/scripts/staging.py

## Make final data, clean and set up
python /tempdata/workdir/gbifprocessing/scripts/make_db.py

## Delete script separated for safety
python /tempdata/workdir/gbifprocessing/scripts/delete_rows.py

## Create summary tables
python /tempdata/workdir/gbifprocessing/scripts/make_summary_tables.py

