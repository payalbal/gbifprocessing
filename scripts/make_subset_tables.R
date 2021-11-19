## Load libraries
# install.packages("pacman")
x = c("DBI", "RPostgreSQL")
lapply(x, require, character.only = TRUE)
rm(x)


## Connect to server
source("/tempdata/workdir/gsdms/scripts/connect_to_server.R")


## Create subset table
dbname <- "gbif_aves"
spgroup <- "Aves"
keyname <- "aves_pk"
idx_prefix <- "aves"

dbSendQuery(con, sprintf("
                  drop table if exists %s; 
                  create table %s AS 
                  select * 
                  from clean_gbif 
                  where taxclass = %s;
                  
                  alter table %s add constraunt %s primary key ( gbifid );",
                         dbname, dbname, spgroup, dbname, keyname))

print("number of rows in subset db: ")
dbGetQuery(con, sprintf("
                        SELECT reltuples::bigint AS estimate 
                        FROM pg_class WHERE  oid = 'public.%s'::regclass;", 
                        dbname))



## Create indices
dbSendQuery(con, sprintf("create index %s_species_idx on %s (species);", 
                         idx_prefix, dbname))
dbSendQuery(con, sprintf("create index %s_taxonkey_idx on %s (taxonkey);", 
                         idx_prefix, dbname))
dbSendQuery(con, sprintf("create index %s_scname_idx on %s (scientificname);", 
                         idx_prefix, dbname))
dbSendQuery(con, sprintf("create index %s_year_idx on %s (recyear);", 
                         idx_prefix, dbname))
dbSendQuery(con, sprintf("create index %s_country_idx on %s (countrycode);", 
                         idx_prefix, dbname))
dbSendQuery(con, sprintf("create index %s_phylum_idx on %s (phylum);", 
                         idx_prefix, dbname))
dbSendQuery(con, sprintf("create index %s_taxorder_idx on %s (taxorder);", 
                         idx_prefix, dbname))
dbSendQuery(con, sprintf("create index %s_taxfamily_idx on %s (taxfamily);", 
                         idx_prefix, dbname))



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
		      order by spcounts DESC;", dbname, "_counts", dbname, "_counts", dbname))

dbSendQuery(con, sprintf("alter table %s%s add constraint species_pk primary key ( species );",
                         dbname, "_counts"))
dbSendQuery(con, sprintf("create index db_spcounts_idx on %s%s(spcounts);", dbname, "_counts"))

message(sprintf("Number of unique species in %s: ", dbname))
dbGetQuery(con, sprintf("select count(*) from %s%s;", dbname, "_counts"))

aves_splist <- dbGetQuery(con, sprintf("select species from %s%s;", dbname, "_counts"))
aves_splist <- aves_splist$species
write.csv(aves_splist, file = "/tempdata/research-cifs/uom_data/gsdms_data/gbif_clean/aves_splist.csv",
          row.names = FALSE)
