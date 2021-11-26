## GBIF backbone





## Set working environment ####
rm(list = ls())
gc()
# system("ps")
# system("pkill -f R")


x = c("DBI", "RPostgreSQL", "data.table")
lapply(x, require, character.only = TRUE)
rm(x)

gsdms_dir <- "/home/payalb/gsdms_r_vol/tempdata/research-cifs/uom_data/gsdms_data"
nesp_dir <- "/home/payalb/gsdms_r_vol/tempdata/research-cifs/uom_data/nesp_bugs_data"

## Connect to server
source("~/gsdms_r_vol/tempdata/workdir/gbifprocessing/scripts/connect_to_server.R")


## Read in backbone taxonomy 
backbone <- fread(file.path(gsdms_dir, "gbif/backbone/Taxon.tsv"))

names(backbone) <- gsub("class", "taxclass", names(backbone))
names(backbone) <- gsub("order", "taxorder", names(backbone))
names(backbone) <- gsub("family", "taxfamily", names(backbone))



## Remove improper names ####
source("~/gsdms_r_vol/tempdata/workdir/nesp_bugs/scripts/remove_improper_names.R")
species_record <- remove_improper_names(as.character(backbone$canonicalName),
                                        allow.higher.taxa = FALSE,
                                        allow.subspecies = TRUE)
sum(is.na(species_record$updated_list))
str(species_record)

## Update AFD taxonomy based on selected species
species <- as.character(na.omit(species_record$updated_list))
backbone <- backbone[which(backbone$canonicalName %in% species),]
dim(backbone)
## Note:  mismatch between length(species) and dim(backbone), 
##        possibly due to duplicates being removed. 

## Checks for special characters
length(backbone$canonicalName[grep("\"", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("\'", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("(", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("[", backbone$canonicalName, fixed = TRUE)])

fwrite(backbone, file.path(gsdms_dir, "outputs/gbif_clean/backbone_taxonomy.csv"))

## Remove invasive species ####
# ## Source 1: Global_Register_of_Introduced_and_Invasive_Species_Australia XXXX ??
# ## https://lists.ala.org.au/speciesListItem/list/dr9884#list
# griis_species <- read.csv(file.path(gsdms_dir, "gbif/invasives/GRIIS_Global_Register_of_Introduced_and_Invasive_Species_Australia.csv"))
# 
# message(cat("Number of GBIF species listed in GRIIS: "),
#         length(which(backbone$canonicalName %in% griis_species$Supplied.Name)))
# 
# message("GBIF species in GRIIS - Global Register of Introduced and Invasive Species - Australia: ")
# backbone$canonicalName[which(backbone$canonicalName %in% griis_species$Supplied.Name)]
# 
# message("Removing species in GRIIS ...")
# backbone <- backbone[which(backbone$canonicalName %in% griis_species$Supplied.Name),]

## Source 2:  Global Invasive Species Database (GISD)
## https://www.gbif.org/dataset/b351a324-77c4-41c9-a909-f30f77268bc4
gisd <- fread(file.path(gsdms_dir, "gbif/invasives/invasives_2011-11-20/taxon.txt"))$V2

message(cat("Number of GBIF species listed in GISD: "),
        length(which(backbone$canonicalName %in% gisd)))

message("GBIF species in GISD: ")
backbone$canonicalName[which(backbone$canonicalName %in% gisd)]

message("Removing species listed in GISD ...")
backbone <- backbone[which(!backbone$canonicalName %in% gisd),]
dim(backbone)




## ------------------------------ ##
## Identify duplicates ####
## ------------------------------ ##

## List duplicates comparing all columns
##  Note: # gives zero = no duplicated rows
# backbone[duplicated(backbone),] 

## Look for duplicates in specific columns
sum(is.na(backbone$canonicalName))
length(backbone$canonicalName)
length(unique(backbone$canonicalName))
length(unique(backbone$scientificName))
## JM - use COMPLETE_NAME for finding duplicates

## Duplicates in COMPLETE_NAME (excluding first appearance)
message(cat("Number of duplicated COMPLETE_NAME (excluding first appearance): "), 
        length(backbone$COMPLETE_NAME[duplicated(backbone$COMPLETE_NAME)]))
message("duplicated COMPLETE_NAME: ")
backbone$COMPLETE_NAME[duplicated(backbone$COMPLETE_NAME)]

## Duplicates in COMPLETE_NAME (including first appearance)
temp <- backbone[which(
  duplicated(backbone$COMPLETE_NAME) | 
    duplicated(backbone$COMPLETE_NAME[
      length(backbone$COMPLETE_NAME):1])
  [length(backbone$COMPLETE_NAME):1]),]

message(cat("#duplicates in COMPLETE_NAME (including first appearance) : ", dim(temp)))
message("duplicated COMPLETE_NAME: ")
temp$COMPLETE_NAME
# readr::write_csv(temp, "./output/afd_completename_repeats.csv")

## Resolve duplicates from ALA list *in consultation with JM*
## using TAXON_GUID from afd_completename_repeats_JRM.csv
# backbone <- unique(backbone$COMPLETE_NAME)
removed_dups <- c("b05771ae-bda7-497a-87c4-b55a0ebc4ca1",
                  "03acc9d4-a209-4bf0-9972-bc7d35d56aea",
                  "83d18631-e160-42ad-8332-89e4b8ba82b6",
                  "c05506f8-0188-4850-8136-7b45ea35638e",
                  "0bb19498-874f-4c6c-a637-124ec9878130")

backbone <- backbone[which(backbone$TAXON_GUID %!in% removed_dups),]
readr::write_csv(backbone, "./outputs/backbone_clean.csv")

## Checks
sp_words <- sapply(strsplit(as.character(backbone$canonicalName), " "), length)
length(backbone$canonicalName[which(sp_words == 5)])
length(backbone$canonicalName[which(sp_words == 4)])
length(backbone$canonicalName[which(sp_words == 3)])
length(backbone$canonicalName[which(sp_words == 2)])
length(backbone$canonicalName[which(sp_words == 1)])

## Checks
length(backbone$canonicalName[grep("\"", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("\'", backbone$canonicalName, fixed = TRUE)])











## Create db
dbSendQuery(con, paste0("DROP TABLE IF EXISTS taxonomy;
            CREATE TABLE taxonomy ( ", 
                        paste0(names(backbone), 
                               " TEXT", 
                               collapse = ", "),
                        " );"))

## https://www.postgresqltutorial.com/import-csv-file-into-posgresql-table/



## Backbone taxonomy - TO UPDATE?
## Remove records with scientific names not included in gbif backbone taxonomy
## 'canonicalName' is the 'scientificName' without authorship
data_dir <- "/home/payalb/gsdms_r_vol/tempdata/research-cifs/uom_data/gsdms_data"
backbone <-data.table::fread(file.path(data_dir, "gbif/Taxon.tsv"))


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