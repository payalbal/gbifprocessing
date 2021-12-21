## Create GBIF postgres database ####

## >> Running python scripts from terminal ####
## Activate python environment
system("source /home/ubuntu/environments/gsdms_env/bin/activate") 

## Download the data in raw csv from gbif.org (the steps will work for both full count csv as well as selected row csv)
## citation: https://doi.org/10.15468/dl.e3jr2r
## download link: http://api.gbif.org/v1/occurrence/download/request/0020324-191105090559680.zip
system("python downloadcsv.py")

## Make staging data
system("python staging.py")

## Make final data, clean and set up
system("python make_db.py")

## Delete script separated for safety
system("python delete_rows.py")

## Create summary tables
system("python make_summary_tables.py")


## >> Running python scripts from R ####
## Note: System call to bash file which 
## (1) activates python environment and 
## (2) calls python script files, sparate file as per steps listed above
system("bash /tempdata/workdir/gbifprocessing/scripts/run_python_scripts.sh")

## NOTE: 
## source might does not work directly in R ...
## see https://stackoverflow.com/questions/13702425/source-command-not-found-in-sh-shell
