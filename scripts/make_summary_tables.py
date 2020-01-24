import psycopg2

connection = psycopg2.connect(
    host="",
    database="",
    user="",
    password='',
)
connection.autocommit = True



def summarize_db_by_country( connection ):
  with connection.cursor() as cursor:
      cursor.execute("""
          DROP TABLE IF EXISTS country_counts;
          CREATE TABLE country_counts AS
          SELECT countrycode, COUNT(species) AS numspecies
          FROM clean_gbif
          GROUP BY countrycode;
      """)
      connection.commit()
      cursor.execute("create index db_numspecies_bycountry_idx on country_counts(numspecies);")
      cursor.execute("update country_counts set countrycode = 'Uknown' where countrycode IS NULL;")
      cursor.execute("alter table country_counts add constraint country_ct_pk primary key ( countrycode );")

summarize_db_by_country( connection )

  # ## To run directly in terminal:
  # tmux new -s counntry_counts
  # login to qaeco_spatial...
  # run queries directly + need to granrt access after creating table
  # GRANT SELECT ON public.country_counts TO PUBLIC;
  
  

def summarize_db_by_phylum( connection ):
  with connection.cursor() as cursor:
      cursor.execute("""
          DROP TABLE IF EXISTS phylum_counts;
          CREATE TABLE phylum_counts AS
          SELECT phylum, COUNT(species) AS numspecies
          FROM clean_gbif
          GROUP BY phylum;
      """)
      connection.commit()
      cursor.execute("create index db_numbyphylum_idx on phylum_counts(numspecies);")
      cursor.execute("update phylum_counts set phylum = 'Uknown' where phylum IS NULL;")
      cursor.execute("alter table phylum_counts add constraint phylum_ct_pk primary key ( phylum );")

summarize_db_by_phylum( connection )



def summarize_db_by_taxclass( connection ):
  with connection.cursor() as cursor:
      cursor.execute("""
          DROP TABLE IF EXISTS taxclass_counts;
          CREATE TABLE taxclass_counts AS
          SELECT taxclass, COUNT(species) AS numspecies
          FROM clean_gbif
          GROUP BY taxclass;
      """)
      connection.commit()
      cursor.execute("create index db_numbyclass_idx on taxclass_counts(numspecies);")
      cursor.execute("update taxclass_counts set taxclass = 'Uknown' where taxclass IS NULL;")
      cursor.execute("alter table taxclass_counts add constraint taxclass_ct_pk primary key ( taxclass );")

summarize_db_by_taxclass( connection )



def summarize_db_by_taxorder( connection ):
  with connection.cursor() as cursor:
      cursor.execute("""
          DROP TABLE IF EXISTS taxorder_counts;
          CREATE TABLE taxorder_counts AS
          SELECT taxorder, COUNT(species) AS numspecies
          FROM clean_gbif
          GROUP BY taxorder;
      """)
      connection.commit()
      cursor.execute("create index db_numbyorder_idx on taxorder_counts(numspecies);")
      cursor.execute("update taxorder_counts set taxorder = 'Uknown' where taxorder IS NULL;")
      cursor.execute("alter table taxorder_counts add constraint taxorder_ct_pk primary key ( taxorder );")

summarize_db_by_taxorder( connection )




def summarize_db_by_taxfamily( connection ):
  with connection.cursor() as cursor:
      cursor.execute("""
          DROP TABLE IF EXISTS taxfamily_counts;
          CREATE TABLE taxfamily_counts AS
          SELECT taxfamily, COUNT(species) AS numspecies
          FROM clean_gbif
          GROUP BY taxfamily;
      """)
      connection.commit()
      cursor.execute("create index db_numbyfamily_idx on taxfamily_counts(numspecies);")
      cursor.execute("update taxfamily_counts set taxfamily = 'Uknown' where taxfamily IS NULL;")
      cursor.execute("alter table taxfamily_counts add constraint taxfamily_ct_pk primary key ( taxfamily );")

summarize_db_by_taxfamily( connection )


