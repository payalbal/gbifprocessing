## Create GBIF subset tables
## -- set up R scripts as jobs with arguments & in parallel for each subset.... (see notes below)


## Set working environment ####
rm(list = ls())
gc()

x = c("DBI", "RPostgreSQL")
lapply(x, require, character.only = TRUE)
rm(x)


## Connect to server
source("~/gsdms_r_vol/tempdata/workdir/gbifprocessing/scripts/connect_to_server.R")


## Specify subset class
tax.class <- "Reptilia"
# tax.class <- "Mammalia"
# tax.class <- "Aves"


## Database strings
dbname <- paste0("gbif_", tolower(tax.class))
keyname <- paste0(tolower(tax.class), "_pk")



## Create table
dbSendQuery(con, sprintf("
                  drop table if exists %s;

                  create table %s AS
                  select *
                  from clean_gbif
                  where taxclass = '%s';",
                         dbname, dbname, tax.class))


print("number of rows in subset db: ")
dbGetQuery(con, sprintf("
                        SELECT reltuples::bigint AS estimate
                        FROM pg_class WHERE  oid = 'public.%s'::regclass;",
                        dbname))



## Add primary key
dbSendQuery(con, sprintf("alter table %s add constraint %s primary key ( gbifid );",
                         dbname, keyname))



## Create indices
dbSendQuery(con, sprintf("create index %s_species_idx on %s (species);",
                         tolower(tax.class), dbname))
dbSendQuery(con, sprintf("create index %s_taxonkey_idx on %s (taxonkey);",
                         tolower(tax.class), dbname))
dbSendQuery(con, sprintf("create index %s_scname_idx on %s (scientificname);",
                         tolower(tax.class), dbname))
dbSendQuery(con, sprintf("create index %s_year_idx on %s (recyear);",
                         tolower(tax.class), dbname))
dbSendQuery(con, sprintf("create index %s_country_idx on %s (countrycode);",
                         tolower(tax.class), dbname))
dbSendQuery(con, sprintf("create index %s_phylum_idx on %s (phylum);",
                         tolower(tax.class), dbname))
dbSendQuery(con, sprintf("create index %s_taxorder_idx on %s (taxorder);",
                         tolower(tax.class), dbname))
dbSendQuery(con, sprintf("create index %s_taxfamily_idx on %s (taxfamily);",
                         tolower(tax.class), dbname))



## Delete records without species information ####
dbSendQuery(con, sprintf("delete from %s where scientificname = 'Unknown';",
                         dbname))
dbSendQuery(con, sprintf("delete from %s where species = 'Unknown';", dbname))
dbSendQuery(con, sprintf("delete from %s where genus = 'Unknown';", dbname))
dbSendQuery(con, sprintf("update %s set taxorder = 'Unknown' where taxorder is null;", dbname))
dbSendQuery(con, sprintf("update %s set taxfamily = 'Unknown' where taxfamily is null;", dbname))







## NOTES ON SETTING UP BATCH SCRIPTS
# Batch submission file: https://github.com/Doi90/JSDM_Prediction/blob/master/scripts/slurm/master_submit.slurm
# Job submission file (called repeatedly by the former): https://github.com/Doi90/JSDM_Prediction/blob/master/scripts/slurm/master_slurm.slurm
# R master script (called once per job submission file call): https://github.com/Doi90/JSDM_Prediction/blob/master/scripts/master/JSDM_Prediction_Master.R
# 
# * 		the first one looped over my different combos in bash, and called the second script once per combo
# * 		second script (also in bash) creates the system call (L26) which is the important bit. The CLAs are the bits like $model following the filename
# * 		the R script uses the commandArgs function to pull the CLAs into a vector that you can then use however you wish to control how the script works (with if statements or switch or whatever)