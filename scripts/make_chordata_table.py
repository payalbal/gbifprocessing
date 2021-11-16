import psycopg2

connection = psycopg2.connect(
  host="",
  database="",
  user="",
  password='',
)
connection.autocommit = True

def chordata_table( connection ):
  with connection.cursor() as cursor:
      cursor.execute("""
          drop table if exists chordata_gbif;
          create table chordata_gbif AS
          select *
          from clean_gbif
          where phylum = 'Chordata';
      """)
      connection.commit()

      cursor.execute("drop index if exists db_chord_species_idx; create index db_chord_species_idx on chordata_gbif(species);")
      cursor.execute("drop index if exists db_chord_taxon_key_idx; create index db_chord_taxon_key_idx on chordata_gbif(taxonkey);")
      cursor.execute("drop index if exists db_chord_scname_idx; create index db_chord_scname_idx on chordata_gbif(scientificname);")
      cursor.execute("drop index if exists db_chord_year_idx; create index db_chord_year_idx on chordata_gbif(recyear);")
      cursor.execute("drop index if exists db_chord_country_idx; create index db_chord_country_idx on chordata_gbif(countrycode);")
      cursor.execute("drop index if exists db_chord_phylum_idx; create index db_chord_phylum_idx on chordata_gbif(phylum);")
      cursor.execute("drop index if exists db_chord_taxclass_idx; create index db_chord_taxclass_idx on chordata_gbif(taxclass);")
      cursor.execute("drop index if exists db_chord_taxorder_idx; create index db_chord_taxorder_idx on chordata_gbif(taxorder);")
      cursor.execute("drop index if exists db_chord_taxfamily_idx; create index db_chord_taxfamily_idx on chordata_gbif(taxfamily);")
      cursor.execute("alter table chordata_gbif add constraint chord_gbifid_pk primary key ( gbifid );")


chordata_table( connection )
