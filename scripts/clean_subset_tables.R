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
dbname = "gbif_mammalia"
dbname = "gbif_reptilia"


# ## (Trial db) ####
# dbSendQuery(con, sprintf("
#             DROP TABLE IF EXISTS aves_temp;
#             CREATE TABLE aves_temp AS
#             SELECT *
#             FROM gbif_aves
#             LIMIT 500000;"))


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

## Check
dbSendQuery(con, sprintf("
            DROP TABLE IF EXISTS temp_spcounts;
            CREATE TABLE temp_spcounts AS
            SELECT species, count(1) spcounts
            FROM %s
            GROUP BY species;", dbname))
dbSendQuery(con, "ALTER TABLE temp_spcounts add constraint tempsp_ct_pk primary key ( species );")

message(cat("species with <=20 records removed: "),
        nrow(dbGetQuery(con, sprintf("
            SELECT species
            FROM %s
            WHERE species IN (
            SELECT species
            FROM temp_spcounts
            WHERE spcounts <= 20)", dbname))) == 0)

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




# ## ------
# ## TO ADD TO PPM CODE ####
# ## Filter by spatial domain 
# if(!is.null(domain.mask)){
#   
#   ## Filter by extent 
#   dat <- dat[which(decimallongitude > domain.mask@extent@xmin)]
#   dat <- dat[which(decimallongitude < domain.mask@extent@xmax)]
#   dat <- dat[which(decimallatitude > domain.mask@extent@ymin)]
#   dat <- dat[which(decimallatitude < domain.mask@extent@ymax)]
#   
#   ## Filter by location on spatial grid (remove points falling outside of mask)
#   sp <- SpatialPoints(dat[,.(decimallongitude,decimallatitude)])
#   grd.pts<-extract(domain.mask, sp)
#   dat <- dat[!is.na(grd.pts),]
#   
# } else {
#   warning("domain.mask not provided")
# }
# 
# ## Remove spatial duplicates if TRUE
# ## identified by species name and coordinates to give only one record for a location
# if(remove_duplicates == TRUE){
#   dat <- unique(dat, by =c("species", "decimallongitude", "decimallatitude"))
# }
# 
# 
# ## Retain species with >= 20 occurrences
# dat <- dat[dat$species %in% names(which(table(dat$species) >= 20)),]
# 
# ## Create cleaned data file for modelling + log file of how it was created
# if(is.null(select_fields)){
#   select_fields = c("gbifid", "species", "decimallatitude", "decimallongitude", "taxonkey")
# } else {
#   check_fields = all(select_fields %in% names(dat))
#   if(!check_fields){
#     select_fields = c("gbifid", "species", "decimallatitude", "decimallongitude", "taxonkey", "issue")
#     warning("Specified select_fields were not found in the dataset - returning default fields instead")
#   } else {
#     select_fields = c(select_fields)
#   }
# }
# dat <- dat[, select_fields, with = FALSE]
# 
# ## Write the data to file, if an output folder is specified
# if(!is.null(output_folder))
# {
#   if(!dir.exists(output_folder))
#   {
#     dir.create(output_folder)
#   } # end if !dir.exists
#   
#   output_path <- file.path(output_folder,paste0(Sys.Date(), "_", output_name,".csv"))
#   write.csv(dat, output_path, row.names=FALSE)
#   
#   ## Write a log file describing how the data was created *************************************
#   fileConn<-file(file.path(output_folder,paste0(output_name,"_",Sys.Date(),"_log_file.txt")),'w')
#   writeLines("#######################################################################",con = fileConn)
#   writeLines("###",con = fileConn)
#   writeLines("### GBIF data filtration log file ",con = fileConn)
#   writeLines("###",con = fileConn)
#   writeLines(paste0("### Created ",Sys.time()," using the filter_gbif_data() function."),con = fileConn)
#   writeLines("###",con = fileConn)
#   writeLines("#######################################################################",con = fileConn)
#   writeLines("",con = fileConn)
#   writeLines(paste0("Output data file = ", output_path),con = fileConn)
#   writeLines(paste0("Domain mask applied = ", domain.mask@file@name),con = fileConn)
#   writeLines(paste0("Data restricted to after ", start.year),con = fileConn)
#   writeLines(paste0("Data restricted to spatial uncertainty < ",spatial.uncertainty.m, "m"),con = fileConn)
#   writeLines(paste0("Spatial duplicates ", if(remove_duplicates == TRUE){"removed"}else{"retained"},con = fileConn))
#   writeLines(paste0("Number of records before filtering = ", n.rec.start),con = fileConn)
#   writeLines(paste0("Number of records after filtering = ", nrow(dat)),con = fileConn)
#   writeLines("#######################################################################",con = fileConn)
#   close(fileConn) 
#   ## *****************************************************************************************
# } # end !is.null(output_folder)
# 
# # write some feedback to the terminal
# if(verbose)
# {
#   msg1 = 'Returned object is a data.table'
#   msg2 = paste('These data have been also been written to ', output_path)
#   msg3 = paste("# records in raw data = ", n.rec.start)
#   msg4 = paste("# records in filtered data = ", dim(dat)[1])
#   msg5 = paste("# records removed =", n.rec.start-dim(dat)[1])
#   msg6 = paste0("Spatial duplicates ", if(remove_duplicates == TRUE){"removed"}else{"retained"})
#   cat(paste(msg1, msg2, msg3, msg4, msg5, msg6, sep = '\n'))
# } # end if(verbose)
# 
# return(dat)
# 
# 
# 
# ## NOT VALID BECAUSE FIELD NOT RETAINED IN DATABASE
# ## Filter by coordinate uncertainty (if 'coordinateuncertaintyinmeters' field is provided)
# if("coordinateuncertaintyinmeters" %in% filter_fields){
#   dat <- dat[!which(coordinateuncertaintyinmeters > spatial.uncertainty.m)]}
# ## note: !which() retains NAs unline which()
# 
# ## Filter records by coordinate precision i.e. records with less than 2 decimal places
# dat <- dat[!which(sapply((strsplit(sub('0+$', '', as.character(dat$decimallongitude)), ".", fixed = TRUE)), function(x) nchar(x[2])) < 2)]
# dat <- dat[!which(sapply((strsplit(sub('0+$', '', as.character(dat$decimallatitude)), ".", fixed = TRUE)), function(x) nchar(x[2])) < 2)]