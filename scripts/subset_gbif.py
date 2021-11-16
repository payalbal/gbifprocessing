## Create subset table - ...

import psycopg2

connection = psycopg2.connect(
  host="localhost",
  database="qaeco_spatial",
  user="qaeco",
  password='B10D1v@rs1tyR0cks!!',
)
connection.autocommit = True



# def subset_db( connection ):
#   with connection.cursor() as cursor:
#   cursor.execute("""
#           DROP TABLE IF EXISTS gbif_arachnida;
#           CREATE TABLE gbif_arachnida AS
#           SELECT *
#           FROM clean_gbif
#           WHERE taxclass = 'Aves';  
#       """)
# connection.commit()



def make_indices( connection ):
    with connection.cursor() as cursor:
       cursor.execute("create index db_species_idx on gbif_arachnida(species);")
       cursor.execute("create index db_taxonkey_idx on gbif_arachnida(taxonkey);")
       cursor.execute("create index db_scname_idx on gbif_arachnida(scientificname);")
       cursor.execute("create index db_year_idx on gbif_arachnida(recyear);")
       cursor.execute("create index db_country_idx on gbif_arachnida(countrycode);")
       cursor.execute("create index db_phylum_idx on gbif_arachnida(phylum);")
       cursor.execute("create index db_taxclass_idx on gbif_arachnida(taxclass);")
       cursor.execute("create index db_taxorder_idx on gbif_arachnida(taxorder);")
       cursor.execute("create index db_taxfamily_idx on gbif_arachnida(taxfamily);")
       cursor.execute("alter table gbif_arachnida add constraint gbifid_pk primary key ( gbifid );")
connection.commit()



def extra_clean( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
           delete from gbif_arachnida where scientificname = 'Uknown';
           delete from gbif_arachnida where species = 'Uknown';
           delete from gbif_arachnida where genus = 'Uknown';
       """)
       connection.commit()


def make_count_table( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
          DROP TABLE IF EXISTS gbif_arachnida_counts;
          CREATE TABLE gbif_arachnida_counts AS
          SELECT species, COUNT(1) AS records
          FROM gbif_arachnida
          GROUP BY species
		      ORDER BY spcounts DESC;
       """)
       connection.commit()
cursor.execute("create index db_records_idx on gbif_arachnida(records);")
cursor.execute("update gbif_arachnida set taxclass = 'Uknown' where taxclass IS NULL;")
cursor.execute("update gbif_arachnida set taxorder = 'Uknown' where taxorder IS NULL;")
cursor.execute("update gbif_arachnida set taxfamily = 'Uknown' where taxfamily IS NULL;")
cursor.execute("alter table gbif_arachnida add constraint species_pk primary key ( species );")

# delete from gbif_arachnida where taxfamily = 'Uknown';
# delete from gbif_arachnida where taxorder = 'Uknown';

