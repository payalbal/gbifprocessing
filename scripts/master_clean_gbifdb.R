## Processing GBIF databse ####
## Clean backbone taxonoomy and create db
source("/tempdata/workdir/gbifprocessing/scripts/make_taxonomy_db.R")

## Create subset tables
source("/tempdata/workdir/gbifprocessing/scripts/")
-- set up R scripts as jobs with arguments & in parallel for each subset.... (see notes below)

## Clean subset tables
source("/tempdata/workdir/gbifprocessing/scripts/")
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

