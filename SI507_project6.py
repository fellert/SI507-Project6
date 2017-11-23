# Frederick Ellert (fellert)
# 73456825

# Import statements
import psycopg2
import psycopg2.extras
from csv import DictReader


# Write code / functions to set up database connection and cursor here.
conn = psycopg2.connect("dbname='fellert_507project6' user=''")
cur = conn.cursor()

# Write code / functions to create tables with the columns you want and all database setup here.
def create_tables():
    cur.execute("DROP TABLE IF EXISTS Sites")
    cur.execute("DROP TABLE IF EXISTS States")
    commands = (
        """
        CREATE TABLE States (
            ID SERIAL PRIMARY KEY,
            Name VARCHAR(128) UNIQUE
        )
        """,
        """
        CREATE TABLE Sites (
            ID SERIAL PRIMARY KEY,
            Name VARCHAR(128) UNIQUE,
            Type VARCHAR(128),
            State_ID INTEGER REFERENCES States(ID) ON DELETE CASCADE,
            Location VARCHAR(255),
            Description TEXT
        )
        """
    )
    for command in commands:
        cur.execute(command)

create_tables()


def insert_site(sites, state_name):
    reader = DictReader(open(sites, 'r'))
    cur.execute("INSERT INTO States (Name) VALUES (%s) RETURNING ID", (state_name,))
    result = cur.fetchone()
    for r in reader:
        r['STATE_ID'] = result[0]
        cur.execute("INSERT INTO Sites (Name,Type,State_ID,Location,Description) \
                        VALUES (%(NAME)s, %(TYPE)s,%(STATE_ID)s,%(LOCATION)s,%(DESCRIPTION)s)", r)

insert_site('arkansas.csv', 'Arkansas')
insert_site('california.csv', 'California')
insert_site('michigan.csv', 'Michigan')
conn.commit()

# Printer function that is used for three queries below
def print_results(items):
    for i in items:
        print(i[0] or 'None')

# Code that executes queries, saves the results to variables, and prints out the results
all_locations = cur.execute(""" SELECT Location FROM Sites """)
result = cur.fetchall()
print('\n')
print("########## ALL LOCATIONS ##########")
print_results(result)
print('\n')

beautiful_sites = cur.execute(""" SELECT Name FROM Sites WHERE Description ilike '%beautiful%' """)
result = cur.fetchall()
print("########## SITES WHERE DESCRIPTION CONTAINS 'BEAUTIFUL' ##########")
print_results(result)
print('\n')

natl_lakeshores = cur.execute(""" SELECT COUNT(*) FROM Sites WHERE Type = 'National Lakeshore' """)
result = cur.fetchall()
print("########## COUNT OF SITE THAT ARE TYPE 'NATIONAL LAKESHORE' ##########")
print(result[0][0])
print('\n')

michigan_names = cur.execute(""" SELECT Sites.Name FROM Sites INNER JOIN States ON (Sites.State_ID = States.ID) \
                                 WHERE States.Name = 'Michigan' """)
result = cur.fetchall()
print("########## MICHIGAN SITE NAMES ##########")
print_results(result)
print('\n')

total_number_arkansas = cur.execute(""" SELECT COUNT(*) FROM Sites INNER JOIN States ON (Sites.State_ID = States.ID) \
                                        WHERE States.Name = 'Arkansas' """)
result = cur.fetchall()
print("########## TOTAL SITES IN ARKANSAS ##########")
print(result[0][0])

conn.close()
