## I. Create GBIF postgres database ####
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
system("bash /tempdata/workdir/gbifprocessing/scripts/run_python_scripts.sh")

## NOTE: 
## source might does not work directly in R ...
## see https://stackoverflow.com/questions/13702425/source-command-not-found-in-sh-shell




## II. Databse processing ####
## Create subset tables
-- set up R scripts as jobs with arguments & in parallel for each subset.... (see notes below)

## Clean subset tables
-- set up R scripts as jobs with arguments & in parallel for each subset.... (see notes below)

## Filter species in subset tables by GBIF backbone taxonomy



## NOTES (FROM DW)
# Batch submission file: https://github.com/Doi90/JSDM_Prediction/blob/master/scripts/slurm/master_submit.slurm
# Job submission file (called repeatedly by the former): https://github.com/Doi90/JSDM_Prediction/blob/master/scripts/slurm/master_slurm.slurm
# R master script (called once per job submission file call): https://github.com/Doi90/JSDM_Prediction/blob/master/scripts/master/JSDM_Prediction_Master.R
# 
# * 		the first one looped over my different combos in bash, and called the second script once per combo
# * 		second script (also in bash) creates the system call (L26) which is the important bit. The CLAs are the bits like $model following the filename
# * 		the R script uses the commandArgs function to pull the CLAs into a vector that you can then use however you wish to control how the script works (with if statements or switch or whatever)


