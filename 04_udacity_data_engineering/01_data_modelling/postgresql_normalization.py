# docker run --name node_postgres -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=password -d postgres
import psycopg2

try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=password")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)


try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)


# create a table that's not normalized
query = """
CREATE TABLE IF NOT EXISTS music_store 
(id integer, customer_name varchar, 
cashier_name varchar, year integer, 
purchased_albums text[])
"""
cur.execute(query)


query = "SELECT count(*) from music_store"
cur.execute(query)
count = cur.fetchone()

# if no data written, write it
if count[0] == 0:
    query = """
    INSERT 
    INTO music_store (id, customer_name, cashier_name, year, purchased_albums) 
    VALUES (%s, %s, %s, %s, %s)"""

    values = [(1, 'Amanda', 'Sam', 2000, ['Rubber Soul', 'Let it be']), 
                (2, 'Toby', 'Sam', 2000, ['My Generation']), 
                (3, 'Max', 'Bob', 2018, ['Meet the Beatles', 'Help!'])]

    for value in values:
        cur.execute(query, value)


print("Original data")
# select query on the same
query = "SELECT * FROM music_store;"
cur.execute(query)
result = cur.fetchall()
for row in result:
    print(row)


# 1st NF tables

query = """
CREATE TABLE IF NOT EXISTS music_store2 
(transaction_id integer, customer_name varchar, 
cashier_name varchar, year integer, purchased_album text);
"""

cur.execute(query)

query = "SELECT count(*) FROM music_store2;"
cur.execute(query)
count = cur.fetchone()
if not count[0]:
    query = "INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, purchased_album) \
        VALUES (%s, %s, %s, %s, %s)"

    values = [(1, 'Amanda', 'Sam', 2000, 'Rubber Soul'), 
                (1, 'Amanda', 'Sam', 2000, 'Let it be'), 
                (2, 'Toby', 'Sam', 2000, 'My Generation'), 
                (3, 'Max', 'Bob', 2018, 'Meet the Beatles'), 
                (3, 'Max', 'Bob', 2018, 'Help!')]

    for value in values:
        cur.execute(query, value)


print("Data in 1st NF")
query = "SELECT * FROM music_store2;"


cur.execute(query)
result = cur.fetchall()

for row in result:
    print(row)


print("Similarly for 2NF and 3NF")