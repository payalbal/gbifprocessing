## Create GBIF subset tables


## Set working environment ####
rm(list = ls())
gc()

x = c("DBI", "RPostgreSQL")
lapply(x, require, character.only = TRUE)
rm(x)


## Connect to server
source("~/gsdms_r_vol/tempdata/workdir/gsdms/scripts/connect_to_server.R")


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



## Delete records with species information ####
dbSendQuery(con, sprintf("delete from %s where scientificname = 'Uknown';",
                         dbname))
dbSendQuery(con, sprintf("delete from %s where species = 'Uknown';", dbname))
dbSendQuery(con, sprintf("delete from %s where genus = 'Uknown';", dbname))
dbSendQuery(con, sprintf("update %s set taxorder = 'Uknown' where taxorder is null;", dbname))
dbSendQuery(con, sprintf("update %s set taxfamily = 'Uknown' where taxfamily is null;", dbname))



## Create species counts table & add primary key and indices ####
dbSendQuery(con, sprintf("
          drop table if exists %s%s;
          create table %s%s as
          select species, count(1) AS spcounts
          from %s
          group by species
		      order by spcounts DESC;", "spcounts_", tolower(tax.class), "spcounts_", tolower(tax.class), dbname))

dbSendQuery(con, sprintf("alter table %s%s add constraint %s_species_pk primary key ( species );",
                         "spcounts_", tolower(tax.class), tolower(tax.class)))
dbSendQuery(con, sprintf("create index %s_spcounts_idx on %s%s(spcounts);", tolower(tax.class), "spcounts_", tolower(tax.class)))

# message(sprintf("Number of unique species in %s: ", dbname))
# dbGetQuery(con, sprintf("select count(*) from %s%s;", dbname, "_counts"))


# ## Create species list
# splist <- dbGetQuery(con, sprintf("select species from %s%s;", dbname, "_counts"))
# splist <- splist$species
# write.csv(splist, file = sprintf("/tempdata/research-cifs/uom_data/gsdms_data/gbif_clean/%s_splist.csv", tolower(tax.class)), row.names = FALSE)
