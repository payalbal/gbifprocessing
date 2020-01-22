import psycopg2

connection = psycopg2.connect(
    host="",
    database="",
    user="",
    password='',
)
connection.autocommit = True

def create_db_table( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
         DROP TABLE IF EXISTS clean_gbif;
         CREATE TABLE clean_gbif (
             gbifid BIGINT,
             species TEXT,
             scientificname TEXT,
             countrycode TEXT,
             decimallatitude DECIMAL,
             decimallongitude DECIMAL,
             elevation DECIMAL,
             elevationaccuracy DECIMAL,
             recdepth DECIMAL,
             depthaccuracy DECIMAL,
             eventdate TIMESTAMP,
             recyear INT,
             taxonkey INT,
             phylum TEXT,
             taxclass TEXT,
             taxorder TEXT,
             taxfamily TEXT,
             genus TEXT,
             specieskey INT,
             basisofrecord TEXT,
             issue TEXT
         );
       """)

def fill_table( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
            INSERT INTO clean_gbif
            SELECT
            gbifid::BIGINT,
            species,
            NULLIF( scientificname, ''),
            NULLIF( countrycode, ''),
            CAST( NULLIF( decimallatitude, '') AS DECIMAL ),
            CAST( NULLIF( decimallongitude, '') AS DECIMAL ),
            CAST( NULLIF( elevation, '') AS DECIMAL ),
            CAST( NULLIF( elevationaccuracy, '') AS DECIMAL ),
            CAST( NULLIF( "depth", '') AS DECIMAL ),
            CAST( NULLIF( depthaccuracy, '') AS DECIMAL ),
            CAST( NULLIF( eventdate, '') AS TIMESTAMP ),
            CAST( NULLIF( "year", '') AS INT ),
            CAST( NULLIF( taxonkey, '') AS INT ),
            NULLIF( phylum, ''),
            NULLIF( "class", ''),
            NULLIF( "order", ''),
            NULLIF( "family", ''),
            NULLIF( genus, ''),
            CAST( NULLIF( specieskey, '') AS INT ),
            NULLIF( basisofrecord, ''),
            NULLIF( issue, '')
            FROM staging_gbif
            WHERE kingdom = 'Animalia'
            AND year BETWEEN '1950' AND '2018'
            AND basisofrecord IN ('HUMAN_OBSERVATION','PRESERVED_SPECIMEN', 
                                  'OBSERVATION', 'MATERIAL_SAMPLE', 
                                  'MACHINE_OBSERVATION')
            AND issue NOT IN ('ZERO_COORDINATE', 'COORDINATE_INVALID', 
                              'COORDINATE_OUT_OF_RANGE', 
                              'COUNTRY_COORDINATE_MISMATCH', 
                              'COORDINATE_REPROJECTION_FAILED',
                              'COORDINATE_REPROJECTION_SUSPICIOUS',
                              'GEODETIC_DATUM_INVALID',
                              'TAXON_MATCH_FUZZY', 'TAXON_MATCH_HIGHERRANK', 
                              'TAXON_MATCH_NONE',
                              'BASIS_OF_RECORD_INVALID')
            AND species LIKE '% %'
            AND species NOT LIKE '%(sp|sp.|spp|spp.|spec|spec.)%';
       """)
       connection.commit()

def del_dup_gbifid( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
          ALTER TABLE clean_gbif ADD COLUMN is_uniq SERIAL;
          DELETE
          FROM clean_gbif a USING clean_gbif b
          WHERE
          a.gbifid = b.gbifid
          AND a.is_uniq < b.is_uniq;
          ALTER TABLE clean_gbif DROP COLUMN is_uniq;
       """)
       connection.commit()

def make_indices( connection ):
    with connection.cursor() as cursor:
       cursor.execute("create index db_species_idx on clean_gbif(species);")
       cursor.execute("create index db_taxon_key_idx on clean_gbif(taxonkey);")
       cursor.execute("create index db_scname_idx on clean_gbif(scientificname);")
       cursor.execute("create index db_year_idx on clean_gbif(recyear);")
       cursor.execute("create index db_country_idx on clean_gbif(countrycode);")
       cursor.execute("create index db_phylum_idx on clean_gbif(phylum);")
       cursor.execute("create index db_taxclass_idx on clean_gbif(taxclass);")
       cursor.execute("create index db_taxorder_idx on clean_gbif(taxorder);")
       cursor.execute("create index db_taxfamily_idx on clean_gbif(taxfamily);")
       cursor.execute("alter table clean_gbif add constraint gbifid_pk primary key ( gbifid );")
       

def make_count_table( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
          DROP TABLE IF EXISTS species_counts;
          CREATE TABLE species_counts AS
            SELECT species, count(1) numvals
            FROM clean_gbif
            GROUP BY species;
       """)
       connection.commit()
       cursor.execute("alter table species_counts add constraint sp_ct_pk primary key ( species );")

def extra_clean( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
           delete from clean_gbif where species is null or species = '';
           delete from clean_gbif where scientificname is null or scientificname = '';
           delete from clean_gbif where decimallatitude is null;
           delete from clean_gbif where decimallongitude is null;
       """)
       connection.commit()

def make_minor_table( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
          DROP TABLE IF EXISTS low_counts;
          CREATE TABLE low_counts AS
            SELECT *
            FROM clean_gbif 
            WHERE species IN ( 
               SELECT species
               FROM species_counts
               WHERE numvals <= 20
            );
       """)
       connection.commit()
       cursor.execute("alter table low_counts add constraint low_ct_gbifid_pk primary key ( gbifid );")
       #cursor.execute("""
       #   DELETE FROM clean_gbif
       #   WHERE species IN (
       #        SELECT species
       #        FROM species_counts
       #        WHERE numvals <= 20
       #     );
       #""")
       #connection.commit()

create_db_table( connection )
fill_table( connection )
del_dup_gbifid( connection )
make_indices( connection )
extra_clean( connection )
make_count_table( connection )
make_minor_table( connection )
