import psycopg2

connection = psycopg2.connect(
    host="",
    database="",
    user="",
    password='',
)
connection.autocommit = True

def delete_lows( connection ):
    with connection.cursor() as cursor:
       cursor.execute("""
          DELETE FROM clean_gbif
          WHERE species IN (
             SELECT species
             FROM species_counts
             WHERE numvals <= 20
          );
       """)
       connection.commit()
