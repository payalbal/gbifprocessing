## Create PostGIS tables to use with GBIF

## Set working environment ####
rm(list = ls())
gc()

x = c("DBI", "RPostgreSQL", "rgdal", "job")
lapply(x, require, character.only = TRUE)
rm(x)

ogrDrivers()

gsdms_dir <- "/home/payalb/gsdms_r_vol/tempdata/research-cifs/uom_data/gsdms_data"

## Connect to server
source("~/gsdms_r_vol/tempdata/workdir/gbifprocessing/scripts/connect_to_server.R")



## Create SQL file from shapefile (inQGIS) ####
## Right click layer > Export > Save Features As… > PostgreSQL SQL dump; Create schema: No > specify file name
## >> Dinerstein et al 2017 Ecoregionns ####
infile <- "/tempdata/research-cifs/uom_data/gsdms_data/ecoregions_dinerstein_2017/ecoregions_dinerstein_2017.sql"


## Create PostGIS table ####
## https://postgis.net/docs/using_postgis_dbmanagement.html
system(sprintf(paste0("export PGPASSWORD='%s'; psql -h localhost -U qaeco -d qaeco_spatial -f ", infile), pword))







## EXTRAS
# ## To create PostGIS table using writeOGR
# # https://postgis.net/workshops/postgis-intro/loading_data.html
# # https://gis.stackexchange.com/questions/111181/import-shapefiles-on-my-postgis-table-using-r/111192
# x <- readOGR(infile)
# writeOGR(x, "PG:dbname='BDDMeteo' user=user password='' ,host='localhost', port='5432' ", layer_options = "geometry_name=geom", 
#          "newtablename", "PostgreSQL")


