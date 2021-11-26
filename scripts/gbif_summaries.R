## Filter GBIF subset database using SQL queries

## Set working environment ####
rm(list = ls())
gc()

x = c("DBI", "RPostgreSQL")
lapply(x, require, character.only = TRUE)
rm(x)


## Connect to server
source("~/gsdms_r_vol/tempdata/workdir/gsdms/scripts/connect_to_server.R")



## Data by year 
x <- data.table::as.data.table(dbGetQuery(con, "SELECT * FROM public.recyear_counts;"))
x <- x[order(recyear),]
x1 <- x[recyear <= 1980]
x2 <- x[recyear > 1980 & recyear <= 2000]
x3 <- x[recyear > 2000]

par(mfrow = c(1,3))
plot(x1)
plot(x2)
plot(x3)

plot(x)