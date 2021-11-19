## Filter GBIF subset database using SQL queries

dbif.downloaded.data
gbif.nub.taxonomy
subset.gbifnubtaxonomy.byclass = NULL
output_folder = NULL
output_name = "filtereted_gbif"
domain.mask = NULL
start.year = 1950
end.year = 2018
remove_duplicates = TRUE
spatial.uncertainty.m = 1000
issue_date = NULL
select_fields = NULL
verbose = TRUE



## Rename columns
names(dat)[names(dat) == "recyear"] = "year"
names(dat)[names(dat) == "taxclass"] = "class"
names(dat)[names(dat) == "taxorder"] = "order"
names(dat)[names(dat) == "taxfamily"] = "family"


## Catch the original number of records (estimate) ####
SELECT reltuples::bigint AS estimate
FROM pg_class
WHERE  oid = 'public.gbif_aves'::regclass;	  


## Drop unwanted columns ####
filter_fields = c("gbifid", "species", "scientificname", 
                  "countrycode", "decimallatitude", 
                  "decimallongitude", "coordinateuncertaintyinmeters",
                  "year","taxonkey","phylum","class","order","family",
                  "genus","specieskey","basisofrecord","issue", "taxclass")

ALTER TABLE table_name
DROP COLUMN column_name1,
DROP COLUMN column_name2,
...;



## Filter by date range ####
start.year
end.year
delete from clean_gbif where recyear is null or recyear = '' or recyear <= start.year or recyear >= end. year;



## Filter by basis of record ####
filter_basisofrecord = c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN", "OBSERVATION", 
                         "MATERIAL_SAMPLE", "MACHINE_OBSERVATION")
delete from clean_gbif where basisofrecord = '';


## Filter by issues in data ####
issue_geospatial = c("ZERO_COORDINATE", "COORDINATE_INVALID", "COORDINATE_OUT_OF_RANGE", 
                     "COUNTRY_COORDINATE_MISMATCH", "COORDINATE_REPROJECTION_FAILED", 
                     "COORDINATE_REPROJECTION_SUSPICIOUS", "GEODETIC_DATUM_INVALID")
issue_taxonomic = c("TAXON_MATCH_FUZZY", "TAXON_MATCH_HIGHERRANK", "TAXON_MATCH_NONE")
issue_basisofrecord = c("BASIS_OF_RECORD_INVALID")

issues_list <- c(issue_geospatial, issue_taxonomic, issue_basisofrecord)

dbSendQuery(con, paste0("delete from clean_gbif where issue = '", issues_list, "';"))







## Filter by coordinate uncertainty (if 'coordinateuncertaintyinmeters' field is provided)
if("coordinateuncertaintyinmeters" %in% filter_fields){
  dat <- dat[!which(coordinateuncertaintyinmeters > spatial.uncertainty.m)]}
## note: !which() retains NAs unline which()

## Filter records by coordinate precision i.e. records with less than 2 decimal places
dat <- dat[!which(sapply((strsplit(sub('0+$', '', as.character(dat$decimallongitude)), ".", fixed = TRUE)), function(x) nchar(x[2])) < 2)]
dat <- dat[!which(sapply((strsplit(sub('0+$', '', as.character(dat$decimallatitude)), ".", fixed = TRUE)), function(x) nchar(x[2])) < 2)]

## Filter by spatial domain
if(!is.null(domain.mask)){
  
  ## Filter by extent 
  dat <- dat[which(decimallongitude > domain.mask@extent@xmin)]
  dat <- dat[which(decimallongitude < domain.mask@extent@xmax)]
  dat <- dat[which(decimallatitude > domain.mask@extent@ymin)]
  dat <- dat[which(decimallatitude < domain.mask@extent@ymax)]
  
  ## Filter by location on spatial grid (remove points falling outside of mask)
  sp <- SpatialPoints(dat[,.(decimallongitude,decimallatitude)])
  grd.pts<-extract(domain.mask, sp)
  dat <- dat[!is.na(grd.pts),]
  
} else {
  warning("domain.mask not provided")
}

## Remove records with incomplete scientific names i.e. only genus or only species provided
dat <- dat[!which(sapply(strsplit(dat$species, " "), length) < 2)]

## Remove records with incomplete species names i.e. when species is named as sp., spp. sp
dat <- dat[!grep(paste0(c("sp.", "spp", "sp"), "$", collapse = "|"), dat$species, perl = TRUE, value = FALSE)]

## Remove records with scientific names not included in gbif backbone taxonomy
## 'canonicalName' is the 'scientificName' without authorship
gbif.nub.taxonomy <- gbif.nub.taxonomy[, .(taxonID, canonicalName,taxonRank, taxonomicStatus, 
                                           kingdom, phylum, class, order, family, genus)]
if(!is.null(subset.gbifnubtaxonomy.byclass)){
  gbif.nub.taxonomy <- gbif.nub.taxonomy[class==subset.gbifnubtaxonomy.byclass]
} else {
  species_list <- unique(dat$species)
  check_list <- species_list[which(!(species_list%in% unique(gbif.nub.taxonomy$canonicalName)), arr.ind = TRUE)]
  if(!identical(check_list, character(0))){
    dat <- dat[!species %in% check_list]
  } # end !identical(check_list, character(0))    ## cannot catch character(0) with is.null
} # end !is.null(gbif.nub.taxonomy)

## Remove spatial duplicates if TRUE
## identified by species name and coordinates to give only one record for a location
if(remove_duplicates == TRUE){
  dat <- unique(dat, by =c("species", "decimallongitude", "decimallatitude"))
}

## Retain species with >= 20 occurrences
dat <- dat[dat$species %in% names(which(table(dat$species) >= 20)),]

## Create cleaned data file for modelling + log file of how it was created
if(is.null(select_fields)){
  select_fields = c("gbifid", "species", "decimallatitude", "decimallongitude", "taxonkey")
} else {
  check_fields = all(select_fields %in% names(dat))
  if(!check_fields){
    select_fields = c("gbifid", "species", "decimallatitude", "decimallongitude", "taxonkey", "issue")
    warning("Specified select_fields were not found in the dataset - returning default fields instead")
  } else {
    select_fields = c(select_fields)
  }
}
dat <- dat[, select_fields, with = FALSE]

## Write the data to file, if an output folder is specified
if(!is.null(output_folder))
{
  if(!dir.exists(output_folder))
  {
    dir.create(output_folder)
  } # end if !dir.exists
  
  output_path <- file.path(output_folder,paste0(Sys.Date(), "_", output_name,".csv"))
  write.csv(dat, output_path, row.names=FALSE)
  
  ## Write a log file describing how the data was created *************************************
  fileConn<-file(file.path(output_folder,paste0(output_name,"_",Sys.Date(),"_log_file.txt")),'w')
  writeLines("#######################################################################",con = fileConn)
  writeLines("###",con = fileConn)
  writeLines("### GBIF data filtration log file ",con = fileConn)
  writeLines("###",con = fileConn)
  writeLines(paste0("### Created ",Sys.time()," using the filter_gbif_data() function."),con = fileConn)
  writeLines("###",con = fileConn)
  writeLines("#######################################################################",con = fileConn)
  writeLines("",con = fileConn)
  writeLines(paste0("Output data file = ", output_path),con = fileConn)
  writeLines(paste0("Domain mask applied = ", domain.mask@file@name),con = fileConn)
  writeLines(paste0("Data restricted to after ", start.year),con = fileConn)
  writeLines(paste0("Data restricted to spatial uncertainty < ",spatial.uncertainty.m, "m"),con = fileConn)
  writeLines(paste0("Spatial duplicates ", if(remove_duplicates == TRUE){"removed"}else{"retained"},con = fileConn))
  writeLines(paste0("Number of records before filtering = ", n.rec.start),con = fileConn)
  writeLines(paste0("Number of records after filtering = ", nrow(dat)),con = fileConn)
  writeLines("#######################################################################",con = fileConn)
  close(fileConn) 
  ## *****************************************************************************************
} # end !is.null(output_folder)

# write some feedback to the terminal
if(verbose)
{
  msg1 = 'Returned object is a data.table'
  msg2 = paste('These data have been also been written to ', output_path)
  msg3 = paste("# records in raw data = ", n.rec.start)
  msg4 = paste("# records in filtered data = ", dim(dat)[1])
  msg5 = paste("# records removed =", n.rec.start-dim(dat)[1])
  msg6 = paste0("Spatial duplicates ", if(remove_duplicates == TRUE){"removed"}else{"retained"})
  cat(paste(msg1, msg2, msg3, msg4, msg5, msg6, sep = '\n'))
} # end if(verbose)

return(dat)

} # end filter_gbif_data function