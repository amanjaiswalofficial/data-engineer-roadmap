
from cassandra.cluster import Cluster

# PART 1 : Creating multiple tables, as per our query requirements

# connect to cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)


# create keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# set keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# create 1st table, in regards to the first query we'll do
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# create 2nd table, in regards to the second query we'll do
query = "CREATE TABLE IF NOT EXISTS album_library "
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (artist_name,  year))"
try:
    session.execute(query)
except Exception as e:
    print(e)



# insert (multiple run will overwrite record again and again based on primary key) into table
query = "INSERT INTO music_library (year, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s)"

query1 = "INSERT INTO album_library (artist_name, year, album_name)"
query1 = query1 + " VALUES (%s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Who", "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Beatles", 1970, "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Beatles", 1965, "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Who", 1965, "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Monkees", 1966, "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Carpenters", 1970, "Close To You"))
except Exception as e:
    print(e)



# verify data in table 1
print("Data in 1st table")
query = "select * from music_library WHERE YEAR=1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name,)


# verify data in table 2
print("Data in 2nd table")
query = "select * from album_library WHERE ARTIST_NAME='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name, row.year, row.album_name)


# PART 2 - Using partition key and clustering column

query = "DROP TABLE IF EXISTS music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# Creating a new table with primary key as 
# (year, artist_name, album name),
# with year as partition key, and artist name and album name as clustering column
# so, data will be on clusters based on year first
# then will be sorted in desc based on artist name and then album name
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (year, artist_name, album_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)


# insert records
query = "INSERT INTO music_library (year, artist_name, album_name, city)"
query = query + " VALUES (%s, %s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be", "Liverpool"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul", "Oxford"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Who", "My Generation", "London"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees", "Los Angeles"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You", "San Diego"))
except Exception as e:
    print(e)


# query on such table
query = "select * from music_library WHERE YEAR=1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# using partition key as well as cluster column
query = "select * from music_library WHERE YEAR=1970 AND ARTIST_NAME = 'The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)


# ERROR
# since, not all clustering col and pk are used yet
# its not possible to use something else as where filter
print("Will result in error")
query = "select * from music_library WHERE YEAR = 1970 AND LOCATION = 'Liverpool'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
print("Will also result in error, despite using one clustering column")
query = "select * from music_library WHERE YEAR = 1970 AND ARTIST_NAME = 'The Beatles' and LOCATION = 'Liverpool'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


print("Will also result in error, despite using both clustering column")
query = "select * from music_library WHERE YEAR = 1970 AND ARTIST_NAME = 'The Beatles' and ALBUM_NAME = 'Let it Be' and LOCATION = 'Liverpool'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


print("Will work, as only using the partition key and clustering columns")
query = "select city from music_library WHERE YEAR = 1970 AND ARTIST_NAME = 'The Beatles' AND ALBUM_NAME='Let it Be'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.city)

# END