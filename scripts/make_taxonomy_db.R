## GBIF backbone cleaning and db creation
##  GBIF Secretariat (2021). GBIF Backbone Taxonomy. Checklist dataset https://doi.org/10.15468/39omei accessed via GBIF.org on 2021-11-29. 
## Url: https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c 



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


## Clean and save backbone taxonomy CSV ####
## >> Read in backbone taxonomy ####
backbone <- fread(file.path(gsdms_dir, "gbif/backbone/backbone-current/Taxon.tsv"))

names(backbone) <- gsub("class", "taxclass", names(backbone))
names(backbone) <- gsub("order", "taxorder", names(backbone))
names(backbone) <- gsub("family", "taxfamily", names(backbone))


## >> Remove improper names ####
source("~/gsdms_r_vol/tempdata/workdir/nesp_bugs/scripts/remove_improper_names.R")
species_record <- remove_improper_names(as.character(backbone$canonicalName),
                                        allow.higher.taxa = FALSE,
                                        allow.subspecies = TRUE)
sum(is.na(species_record$updated_list))
str(species_record)

species <- as.character(na.omit(species_record$updated_list))
backbone <- backbone[which(backbone$canonicalName %in% species),]
dim(backbone)

## >> Checks for special characters ####
length(backbone$canonicalName[grep("\"", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("\"", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("\'", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("(", backbone$canonicalName, fixed = TRUE)])
length(backbone$canonicalName[grep("[", backbone$canonicalName, fixed = TRUE)])


## >> Examine backbone ####
length(backbone$canonicalName)   
length(unique(backbone$canonicalName))
length(unique(backbone$scientificName))

sum(is.na(backbone$canonicalName))
sum(backbone$canonicalName == "")

sum(is.na(backbone$scientificName))
sum(backbone$scientificName == "")

sum(backbone$genus == "")
sum(backbone$genericName == "")
backbone[genus != ""][genus != genericName][,.(genus, genericName)]

names(backbone)

message(cat("Number of duplicated canonicalName (excluding first appearance): "), 
        length(backbone$canonicalName[duplicated(backbone$canonicalName)]))

message(cat("Number of duplicated scientificName (excluding first appearance): "), 
        length(backbone$scientificName[duplicated(backbone$scientificName)]))


## >> Duplicates in table (comparing all columnns) ####
sum(duplicated(backbone[, c("taxclass", "taxorder", "taxfamily", "genus", "canonicalName")]))


## >> Checks number of words in species name ####
sp_words <- sapply(strsplit(as.character(backbone$canonicalName), " "), length)
unique(sp_words)
length(backbone$canonicalName[which(sp_words == 3)])
length(backbone$canonicalName[which(sp_words == 2)])


## >> Save CSV ####
fwrite(backbone, file.path(gsdms_dir, "outputs/gbif_clean/backbone_taxonomy.csv"))

## Create db ####
## >> Create rmpty table ####
backbone <- fread(file.path(gsdms_dir, "outputs/gbif_clean/backbone_taxonomy.csv"))
dbSendQuery(con, paste0("DROP TABLE IF EXISTS backbone_taxonomy;
            CREATE TABLE backbone_taxonomy ( ", 
                        paste0(names(backbone), 
                               " TEXT", 
                               collapse = ", "),
                        " );"))

## >> Add primary key and indices ####
dbSendQuery(con, "ALTER TABLE backbone_taxonomy add constraint backbone_ct_pk primary key ( taxonID );")

dbSendQuery(con, sprintf("create index backbone_%s_idx on backbone_taxonomy (%s);",
                         "scientificName", "scientificName"))
dbSendQuery(con, sprintf("create index backbone_%s_idx on backbone_taxonomy (%s);",
                         "canonicalName", "canonicalName"))
dbSendQuery(con, sprintf("create index backbone_%s_idx on backbone_taxonomy (%s);",
                         "genericName", "genericName"))
dbSendQuery(con, sprintf("create index backbone_%s_idx on backbone_taxonomy (%s);",
                         "taxclass", "taxclass"))
dbSendQuery(con, sprintf("create index backbone_%s_idx on backbone_taxonomy (%s);",
                         "taxorder", "taxorder"))
dbSendQuery(con, sprintf("create index backbone_%s_idx on backbone_taxonomy (%s);",
                         "taxfamily", "taxfamily"))
dbSendQuery(con, sprintf("create index backbone_%s_idx on backbone_taxonomy (%s);",
                         "genus", "genus"))

## >> Import csv into postgres db ####
taxpath <- tools::file_path_as_absolute(file.path(gsdms_dir, "outputs/gbif_clean/backbone_taxonomy.csv"))
system(sprintf("export PGPASSWORD='%s'; psql -h localhost -U qaeco -d qaeco_spatial -c \"\\copy backbone_taxonomy from '%s' with delimiter ',' csv header;\"", pword, taxpath))

  ## Note: This method does not work..
  # dbSendQuery(con, 
  #   sprintf("COPY backbone_taxonomy
  #             FROM '%s'
  #             DELIMITER ','
  #             CSV HEADER;", 
  #           file.path(gsdms_dir, "outputs/gbif_clean/backbone_temp.csv")))



## Drop 'doubtful' species
## https://www.gbif.org/faq?question=what-does-the-taxon-status-doubtful-mean-and-when-is-used
dbSendQuery(con, sprintf("
            DELETE FROM backbone_taxonomy
            WHERE taxonomicstatus = '%s'
            AND acceptednameusageid is null;", "doubtful"))


## FOR LATER ####
## Invasive species lists ####
## Combine sources and make db. See sources listed in data cleaning worksheet doc. 

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

