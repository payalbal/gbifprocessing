## Filter GBIF subset database using SQL queries

## Set working environment ####
rm(list = ls())
gc()

x = c("DBI", "RPostgreSQL")
lapply(x, require, character.only = TRUE)
rm(x)


## Connect to server
source("~/gsdms_r_vol/tempdata/workdir/gbifprocessing/scripts/connect_to_server.R")


## Filter parameters ####
drop_fields = c()
start.year = 1950
end.year = NULL

## Specify database ####
dbname = "gbif_aves"
# dbname = "gbif_mammalia"
# dbname = "gbif_reptilia"


## Catch the original number of records in db (estimate) ####
N <- dbGetQuery(con, sprintf("
            SELECT reltuples::bigint AS estimate
            FROM pg_class WHERE oid = 'public.%s'::regclass;",
                             dbname))

## Get db column names
db_cols <- dbGetQuery(con, sprintf("
                      SELECT column_name
                      FROM information_schema.columns
                      WHERE table_schema = 'public'
                      AND table_name = '%s';", dbname))$column_name


## Drop unwanted columns ####
if (length(drop_fields) < 0) {
  dbSendQuery(con, sprintf(paste0("
                          ALTER TABLE %s ", 
                                  paste0("DROP COLUMN ", 
                                         db_cols[db_cols %in% drop_fields], 
                                         collapse = ", "),
                                  ";"), dbname))
}


## Filter by date range ####
## Already applied in make_db.py

dbGetQuery(con, sprintf("
            SELECT MIN(recyear) AS min,
            MAX(recyear) AS max
            FROM %s", dbname))

## https://stackoverflow.com/questions/23766084/best-way-to-check-for-empty-or-null-value
if ((!is.null(start.year)) & (!is.null(end.year))) {
  dbSendQuery(con, sprintf("
              DELETE FROM %s WHERE basisofrecord IS NULL or recyear::char = '' or recyear >= %d or recyear <= %d;",
                           dbname, start.year, end.year))
} else if(!is.null(start.year)) {
  dbSendQuery(con, sprintf("
              DELETE FROM %s WHERE recyear IS NULL or recyear::char = '' or recyear <= %d;",
                           dbname, start.year))
} else if(!is.null(end.year)) {
  dbSendQuery(con, sprintf("
              DELETE FROM %s WHERE recyear IS NULL or recyear = '' or recyear >= %d;",
                           dbname, end.year))
}

## Check
dbGetQuery(con, sprintf("
            SELECT MIN(recyear) AS min,
            MAX(recyear) AS max
            FROM %s", dbname))


## Filter by basis of record ####
## Already applied in make_db.py
## https://data-blog.gbif.org/post/living-specimen-to-preserved-specimen-understanding-basis-of-record/
## Darwin core: http://rs.gbif.org/vocabulary/dwc/basis_of_record.xml
## https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/BasisOfRecord.html

dbGetQuery(con, sprintf("
            SELECT DISTINCT(basisofrecord)
            FROM %s", dbname))

lookup_list = c("LITERATURE", "LIVING_SPECIMEN", "UNKNOWN", 
                "FOSSIL_SPECIMEN")

if(!is.null(lookup_list)) {
  dbSendQuery(con, sprintf(paste0("
                          DELETE FROM %s WHERE ",
                                  paste0("basisofrecord = '", 
                                         lookup_list, "'",
                                         collapse = " or "),
                                  ";"), dbname))
} 


## Filter by issues in data ####
## Already applied in make_db.py
## https://data-blog.gbif.org/post/issues-and-flags/

issue_geospatial = c("ZERO_COORDINATE", "COORDINATE_INVALID", "COORDINATE_OUT_OF_RANGE", 
                     "COUNTRY_COORDINATE_MISMATCH", "COORDINATE_REPROJECTION_FAILED", 
                     "COORDINATE_REPROJECTION_SUSPICIOUS", "GEODETIC_DATUM_INVALID")
issue_taxonomic = c("TAXON_MATCH_FUZZY", "TAXON_MATCH_HIGHERRANK", "TAXON_MATCH_NONE")
issue_basisofrecord = c("BASIS_OF_RECORD_INVALID")

lookup_list <- c(issue_geospatial, issue_taxonomic, issue_basisofrecord)

if(!is.null(lookup_list)) {
  dbSendQuery(con, paste0(sprintf("
                          DELETE FROM %s WHERE ", dbname),
                          paste0("issue ILIKE '%", 
                                 lookup_list, "%'",
                                 collapse = " or "),
                          ";"))
} 



## Drop species with <=20 records ####
## Create species counts table
dbSendQuery(con, sprintf("
            DROP TABLE IF EXISTS temp_spcounts;
            CREATE TABLE temp_spcounts AS
            SELECT species, count(1) spcounts
            FROM %s
            GROUP BY species;", dbname))
dbSendQuery(con, "ALTER TABLE temp_spcounts add constraint tempsp_ct_pk primary key ( species );")

## Drop species based on counts
dbSendQuery(con, sprintf("
            DELETE FROM %s
            WHERE species IN (
            SELECT species
            FROM temp_spcounts
            WHERE spcounts <= 20);", dbname))

  # ## Check
  # dbSendQuery(con, sprintf("
  #             DROP TABLE IF EXISTS temp_spcounts;
  #             CREATE TABLE temp_spcounts AS
  #             SELECT species, count(1) spcounts
  #             FROM %s
  #             GROUP BY species;", dbname))
  # dbSendQuery(con, "ALTER TABLE temp_spcounts add constraint tempsp_ct_pk primary key ( species );")

  # ## View temp_speounts
  # SELECT * 
  # FROM temp_spcounts
  # ORDER BY spcounts DESC;

  # message(cat("species with <=20 records removed: "),
  #         nrow(dbGetQuery(con, sprintf("
  #             SELECT species
  #             FROM %s
  #             WHERE species IN (
  #             SELECT species
  #             FROM temp_spcounts
  #             WHERE spcounts <= 20)", dbname))) == 0)

dbSendQuery(con, "DROP TABLE IF EXISTS temp_spcounts;")



## Dealing with synonyms - not needed ####
## Note this is already dealt with in the 'species' column in data
## See email exchange with GBIF helpdesk (Subject: Information on the GBIF backbone taxonomy)
## SQL queries to check example in email
# ## Check backbone for species name
# -- SELECT DISTINCT taxonkey, specieskey
# -- FROM gbif_aves
# -- WHERE scientificname = 'Barnardius barnardi (Vigors & Horsfield, 1827)';
# 
# -- SELECT *
# -- FROM backbone_taxonomy
# -- WHERE scientificname = 'Barnardius barnardi (Vigors & Horsfield, 1827)';
# 
# -- SELECT *
# -- FROM backbone_taxonomy
# -- WHERE taxonid IN ('2479725','2479720', '6170505');
# 
# ## Check dataset for species name, 
# ## note the 'species' column which gives the accpeted name as per backbone 
# -- SELECT COUNT(1)
# -- FROM gbif_aves
# -- WHERE scientificname = 'Barnardius barnardi (Vigors & Horsfield, 1827)';
# 
# -- SELECT *
#   -- FROM gbif_aves
# -- WHERE scientificname = 'Barnardius barnardi (Vigors & Horsfield, 1827)';



## Add points goematry column to GBIF data tables ####
dbSendQuery(con, sprintf("ALTER TABLE %s ADD COLUMN points_geom geometry(Point, 4326);", dbname))
dbSendQuery(con, sprintf("UPDATE %s SET points_geom = ST_SetSRID(ST_MakePoint(decimallongitude, decimallatitude), 4326);", dbname))
dbSendQuery(con, sprintf("CREATE INDEX %s ON public.%s USING GIST (points_geom);", paste0(dbname, "_geom_idx"), dbname))



## Create species counts table & add primary key and indices ####
dbSendQuery(con, sprintf("
          drop table if exists %s%s;
          create table %s%s as
          select species, count(1) AS spcounts
          from %s
          group by species
		      order by spcounts DESC;", "spcounts_", gsub("gbif_", "", dbname), "spcounts_", gsub("gbif_", "", dbname), dbname))

dbSendQuery(con, sprintf("alter table %s%s add constraint %s_species_pk primary key ( species );",
                         "spcounts_", gsub("gbif_", "", dbname), gsub("gbif_", "", dbname)))
dbSendQuery(con, sprintf("create index %s_spcounts_idx on %s%s(spcounts);", gsub("gbif_", "", dbname), "spcounts_", gsub("gbif_", "", dbname)))


## Create species list
splist <- dbGetQuery(con, sprintf("select species from %s%s;", "spcounts_", gsub("gbif_", "", dbname)))
splist <- splist$species
write.csv(splist, file = sprintf("/tempdata/research-cifs/uom_data/gsdms_data/gbif/%s_splist.csv", gsub("gbif_", "", dbname)), row.names = FALSE)



  # ## Display number of records in subset tables ####
  # print(sprintf("number of rows in subset %s db: ", dbname))
  # dbGetQuery(con, sprintf("
  #                         SELECT reltuples::bigint AS estimate
  #                         FROM pg_class WHERE  oid = 'public.%s'::regclass;",
  #                         dbname))
  # 
  # message(sprintf("number of unique species in %s: ", dbname))
  # dbGetQuery(con, sprintf("select count(*) from %s%s;", "spcounts_", gsub("gbif_", "", dbname)))





## EXTRA 
# ## Creating functions
# ## https://www.cybertec-postgresql.com/en/postgresql-count-made-fast/
# dbSendQuery(con, "CREATE FUNCTION row_estimator(query text) RETURNS bigint
#                   LANGUAGE plpgsql AS
#                   $$DECLARE
#                   plan jsonb;
#                   BEGIN
#                   EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;
#                   
#                   RETURN (plan->0->'Plan'->>'Plan Rows')::bigint;
#                   END;$$;")
# 
# ## https://www.citusdata.com/blog/2016/10/12/count-performance/#dup_counts_estimated_filtered
# dbSendQuery(con, "CREATE FUNCTION count_estimate(query text) RETURNS integer 
#                   AS $$
#                   DECLARE
#                     rec   record;
#                     rows  integer;
#                   BEGIN
#                     FOR rec IN EXECUTE 'EXPLAIN ' || query LOOP
#                       rows := substring(rec.\"QUERY PLAN\" FROM ' rows=([[:digit:]]+)');
#                       EXIT WHEN rows IS NOT NULL;
#                     END LOOP;
#                     RETURN rows;
#                   END;
#                   $$ LANGUAGE plpgsql VOLATILE STRICT;
#                   ")


# ## Run as job within script
# job::job(
#   import = NULL,
#   packages = c("DBI", "RPostgreSQL"),
#   {
#     ## Connection to db
#     source("~/gsdms_r_vol/tempdata/workdir/gbifprocessing/scripts/connect_to_server.R")
#     
#     ## Specify table
#     dbname = "gbif_reptilia"
#     
#     ## Run queries
#     dbSendQuery(con, sprintf("ALTER TABLE %s ADD COLUMN points_geom geometry(Point, 4326);", dbname))
#     dbSendQuery(con, sprintf("UPDATE %s SET points_geom = ST_SetSRID(ST_MakePoint(decimallongitude, decimallatitude), 4326);", dbname))
#     dbSendQuery(con, sprintf("CREATE INDEX %s ON public.%s USING GIST (points_geom);", paste0(dbname, "_geom_idx"), dbname))
#   })