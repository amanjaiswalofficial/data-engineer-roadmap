# Post installing postgreSQL cli client and setting env variables for the same
# psql -U postgres
"""
Pre-Req:
Run postgreSQL on docker
Copy *.sql files into the container
Docker exec -it node_postgres /bin/bash
Inside:
psql -U postgres -d pagila -f pagila-schema.sql
psql -U postgres -d pagila -f pagila-data.sql
This loads all the data to the database
"""
import psycopg2

try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=pagila user=postgres password=password")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)


try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)


def run(quer, all=False):
    cur.execute(quer)
    print("Query: \n{}".format(quer))
    print("Output:")

    try:
        if getattr(cur, "description"):
            # print column names
            print([desc[0] for desc in cur.description])
        if all:
            all_output = cur.fetchall()
            print("\n".join([str(output) for output in all_output]), sep="\n")
        else:
            print(cur.fetchone())
    except Exception as err:
        print(err)
    finally:
        print()


# PART - 1
# 3NF schema exploration

run("select count(*) from film")

run("select min(payment_date), max(payment_date) from payment")


query = """
select f.title, p.amount from payment p
join rental r on (p.rental_id=r.rental_id)
join inventory i on (r.inventory_id=i.inventory_id)
join film f on (i.film_id=f.film_id)
limit 5
"""
run(query, all=True)


query = """
select f.title, sum(p.amount) as revenue
from payment p
join rental r on (p.rental_id=r.rental_id)
join inventory i on (r.inventory_id=i.inventory_id)
join film f on (f.film_id=i.film_id)
group by title
order by revenue desc
limit 10
"""

run(quer=query, all=True)


"""
NOTE:
Now to reduce this dependency and complex join logic to get all the data we need
We create dim and fact tables, as part of ETL from the same data as from these
previously existing tables
"""

# PART -2
# CREATING ETL
query = """
CREATE TABLE IF NOT EXISTS dimDate
(
    date_key INTEGER NOT NULL PRIMARY KEY,
    date date NOT NULL,
    year smallint NOT NULL,
    quarter smallint NOT NULL,
    month smallint NOT NULL,
    day smallint NOT NULL,
    week smallint NOT NULL,
    is_weekend boolean
);
"""

run(query)

query = """SELECT column_name, data_type FROM information_schema.columns WHERE table_name   = 'dimdate'"""
run(query)



query = """
CREATE TABLE IF NOT EXISTS dimCustomer
(
  customer_key SERIAL PRIMARY KEY,
  customer_id  smallint NOT NULL,
  first_name   varchar(45) NOT NULL,
  last_name    varchar(45) NOT NULL,
  email        varchar(50),
  address      varchar(50) NOT NULL,
  address2     varchar(50),
  district     varchar(20) NOT NULL,
  city         varchar(50) NOT NULL,
  country      varchar(50) NOT NULL,
  postal_code  varchar(10),
  phone        varchar(20) NOT NULL,
  active       smallint NOT NULL,
  create_date  timestamp NOT NULL,
  start_date   date NOT NULL,
  end_date     date NOT NULL
);

CREATE TABLE IF NOT EXISTS dimMovie
(
  movie_key          SERIAL PRIMARY KEY,
  film_id            smallint NOT NULL,
  title              varchar(255) NOT NULL,
  description        text,
  release_year       year,
  language           varchar(20) NOT NULL,
  original_language  varchar(20),
  rental_duration    smallint NOT NULL,
  length             smallint NOT NULL,
  rating             varchar(5) NOT NULL,
  special_features   varchar(60) NOT NULL
);
CREATE TABLE IF NOT EXISTS dimStore
(
  store_key           SERIAL PRIMARY KEY,
  store_id            smallint NOT NULL,
  address             varchar(50) NOT NULL,
  address2            varchar(50),
  district            varchar(20) NOT NULL,
  city                varchar(50) NOT NULL,
  country             varchar(50) NOT NULL,
  postal_code         varchar(10),
  manager_first_name  varchar(45) NOT NULL,
  manager_last_name   varchar(45) NOT NULL,
  start_date          date NOT NULL,
  end_date            date NOT NULL
);

"""


run(query)


# important: using REFERENCES here
# this uses concept of foreign key and links each col's data to
# a different dim table, which can be used to fetch records later
query = """
CREATE TABLE IF NOT EXISTS factSales
(
    sales_key SERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dimDate (date_key),
    customer_key INTEGER NOT NULL REFERENCES dimCustomer (customer_key),
    movie_key INTEGER NOT NULL REFERENCES dimMovie (movie_key),
    store_key INTEGER NOT NULL REFERENCES dimStore (store_key),
    sales_amount NUMERIC NOT NULL
);
"""

run(query)

# PART - 3
# INSERTING RECORDS FROM 3NF DATABASE
# INTO THE NEW ETL

first_run = False
if first_run:
    query = """
    INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)
    SELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
        date(payment_date)                                           AS date,
        EXTRACT(year FROM payment_date)                              AS year,
        EXTRACT(quarter FROM payment_date)                           AS quarter,
        EXTRACT(month FROM payment_date)                             AS month,
        EXTRACT(day FROM payment_date)                               AS day,
        EXTRACT(week FROM payment_date)                              AS week,
        CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend
    FROM payment;
    """

    run(query)

if first_run:
    query = """
    INSERT INTO dimCustomer (customer_key, customer_id, first_name, last_name, email, address, 
                            address2, district, city, country, postal_code, phone, active, 
                            create_date, start_date, end_date)
    SELECT 
        c.customer_id AS customer_key,
        c.customer_id AS customer_id,
        c.first_name AS first_name,
        c.last_name AS last_name,
        c.email AS email,
        a.address AS address,
        a.address2 AS address2,
        a.district AS district,
        ci.city AS city,
        co.country AS country,
        a.postal_code AS postal_code,
        a.phone AS phone,
        c.active AS active,
        c.create_date AS create_date,
        now()         AS start_date,
        now()         AS end_date
    FROM customer c
    JOIN address a  ON (c.address_id = a.address_id)
    JOIN city ci    ON (a.city_id = ci.city_id)
    JOIN country co ON (ci.country_id = co.country_id);
    """

    run(query)

if first_run:
    query = """
    INSERT INTO dimMovie (movie_key, film_id, title, description,
                        release_year, language, original_language,
                        rental_duration, length, rating, special_features)
    SELECT 
        f.film_id AS movie_key,
        f.film_id AS film_id,
        f.title AS title,
        f.description AS description,
        f.release_year AS release_year,
        l.name AS language,
        orig_lang.name AS original_language,
        f.rental_duration AS rental_duration,
        f.length AS length,
        f.rating AS rating,
        f.special_features AS special_features
    FROM film f
    JOIN language l              ON (f.language_id=l.language_id)
    LEFT JOIN language orig_lang ON (f.original_language_id = orig_lang.language_id);
    """

    run(query)

if first_run:
    query = """
    INSERT INTO dimStore (store_key, store_id, address, address2,
                        district, city, country, postal_code,
                        manager_first_name, manager_last_name,
                        start_date, end_date)
    SELECT 
        s.store_id AS store_key,
        s.store_id AS store_id,
        a.address AS address,
        a.address2 AS address2,
        a.district AS district,
        ci.city AS city,
        co.country AS country,
        a.postal_code AS postal_code,
        st.first_name AS manager_first_name,
        st.last_name AS manager_last_name,
        now()         AS start_date,
        now()         AS end_date
    FROM store s
    JOIN staff st   ON (st.staff_id = s.manager_staff_id)
    JOIN address a  ON (s.address_id = a.address_id)
    JOIN city ci    ON (a.city_id = ci.city_id)
    JOIN country co ON (ci.country_id = co.country_id);
    """

    run(query)

if first_run:
    query = """
    INSERT INTO factSales (date_key, customer_key, movie_key, store_key, sales_amount)
    SELECT 
        TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer AS date_key,
        p.customer_id AS customer_key,
        i.film_id AS movie_key,
        i.store_id AS store_key,
        p.amount AS sales_amount
    FROM
        payment p
        JOIN rental r ON (p.rental_id = r.rental_id)
        JOIN inventory i ON (r.inventory_id = i.inventory_id);
    """

    run(query)


# PART - 4
# RUNNING QUERIES AGAINST THE NEW STAR SCHEMA GENERATED ETL


run("SELECT * FROM dimDate limit 5", all=True)

query = """
SELECT movie_key, date_key, customer_key, sales_amount
FROM factSales 
limit 5;
"""

run(query)

query = """
SELECT dimMovie.title, dimDate.month, dimCustomer.city, sum(sales_amount) as revenue
FROM factSales 
JOIN dimMovie    on (dimMovie.movie_key      = factSales.movie_key)
JOIN dimDate     on (dimDate.date_key         = factSales.date_key)
JOIN dimCustomer on (dimCustomer.customer_key = factSales.customer_key)
group by (dimMovie.title, dimDate.month, dimCustomer.city)
order by dimMovie.title, dimDate.month, dimCustomer.city, revenue desc
"""

run(query)


"""
Major notes:
 > The star schema is easier to understand and write queries against, compared to a 3NF
 > Queries with a star schema are more performant i.e. less time consuming
"""
