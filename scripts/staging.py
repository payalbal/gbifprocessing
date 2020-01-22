import csv
import psycopg2

connection = psycopg2.connect(
    host="",
    database="",
    user="",
    password='',
)
connection.autocommit = True

def create_staging_table( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
          DROP TABLE IF EXISTS staging_gbif;
          CREATE UNLOGGED TABLE staging_gbif (
            gbifid TEXT,
            datasetkey TEXT,
            occurrenceid TEXT,
            kingdom TEXT,
            phylum TEXT,
            class TEXT,
            "order" TEXT,
            family TEXT,
            genus TEXT,
            species TEXT,
            infraspecificepithet TEXT,
            taxonrank TEXT,
            scientificname TEXT,
            verbatimScientificName TEXT,	
            verbatimScientificNameAuthorship TEXT,
            countrycode TEXT,
            locality TEXT,
            stateProvince TEXT,
            occurrenceStatus TEXT,
            individualCount TEXT,
            publishingorgkey TEXT,
            decimallatitude TEXT,
            decimallongitude TEXT,
            coordinateuncertaintyinmeters TEXT,
            coordinateprecision TEXT,
            elevation TEXT,
            elevationaccuracy TEXT,
            depth TEXT,
            depthaccuracy TEXT,
            eventdate TEXT,
            day TEXT,
            month TEXT,
            year TEXT,
            taxonkey TEXT,
            specieskey TEXT,
            basisofrecord TEXT,
            institutioncode TEXT,
            collectioncode TEXT,
            catalognumber TEXT,
            recordnumber TEXT,
            identifiedby TEXT,
            dateIdentified TEXT,
            license TEXT,
            rightsholder TEXT,
            recordedby TEXT,
            typestatus TEXT,
            establishmentmeans TEXT,
            lastinterpreted TEXT,
            mediatype TEXT,
            issue TEXT
          );
       """)

def insert_data(connection):
    with connection.cursor() as cursor:
       with open('/data/csv/0020324-191105090559680.csv', 'r') as f:
           #next(f) #skip header
           #bulk load would be best but there are error rows that make it fail halfway through
           #cursor.copy_from(f, 'staging_gbif', null="")

           reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
           next(reader)
           for fields in reader:
              fields = [ fld if fld != '' else None for fld in fields ]
              if len( fields ) != 50:
                  continue
              substr = ', '.join( ['%s'] * 50 )
              sql = "insert into staging_gbif values (" + substr + ")"
              #print( fields )
              cursor.execute(sql, fields)
              connection.commit()

def make_staging_indices( connection ):
    with connection.cursor() as cursor:
       cursor.execute("create index species_idx on staging_gbif(species);")
       cursor.execute("create index bor_idx on staging_gbif(basisofrecord);")
       cursor.execute("create index issue_idx on staging_gbif(issue);")

create_staging_table( connection )
insert_data( connection )
make_staging_indices( connection )
