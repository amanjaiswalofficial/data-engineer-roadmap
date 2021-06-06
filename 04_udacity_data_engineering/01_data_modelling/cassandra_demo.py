# REQUIREMENT: docker run --name node_cassandra -p 127.0.0.1:9042:9042 -p 127.0.0.1:9160:9160 -d cassandra
from cassandra.cluster import Cluster

try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as err:
    cluster = None
    raise err


# create keyspace, which will contain all tables/records
query = """
create keyspace 
if not exists demo 
with replication={'class' : 'SimpleStrategy', 'replication_factor':1}"""
session.execute(query)

# set that keyspace to use
session.set_keyspace("demo")

# create table command
query = """
create table if not exists 
music_library 
(year int, artist_name text, 
album_name text, 
primary key (year, artist_name))"""

"""
IMP: primary key is what decides how partitioning of data is done
here, since general queries would be made (assumption) over year
so year is chosen as part of primary key
and to make it unique, artist name with year is used as primary key

The reason being, cassandra don't have duplicate rows
so, unique primary key ensures even if new entry with similar value comes
it will replace the previous one
"""

session.execute(query)

query = """
select * from music_library
"""

count = session.execute(query).one() # will return 0 if no record exist

# query to insert command
query = """
insert into music_library (year, artist_name, album_name) values ({}, '{}', '{}')
"""

values = [  (2011, "Skrillex", "Rock N Roll Will Take You To Mountain"),
            (2017, "Linkin Park", "One More Light"),
            (2018, "half alive", "ok not ok")]


"""
Can run this insert query as many times with no primary key error
Since cassandra will always replace data and overwrite over it
"""
for value in values:
    base_query = query.format(*value)
    session.execute(base_query)


query = """
select * from music_library
"""

rows = session.execute(query)

for row in rows:
    print(row.year, row.artist_name, row.album_name)
