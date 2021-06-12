"""
Prerequisite
1. Create Role with access to services like Redshift/S3
2. Create Security Group in EC2's console with following info
    Type: Custom TCP Rule
    Protocol: TCP
    Port Range: 5439
    Source: 0.0.0.0/0
3. Create IAM User and assign access to Redshift/S3
4. Create Redshift Cluster, add security group from 2 and role from 1st step to its network/security
5. Create a S3 Bucket
"""

import boto3
import psycopg2

ACCESS_KEY = "ACCESS_KEY_HERE"
SECRET_KEY = "SECRET_KEY_HERE"
DB_NAME = "REDSHIFT_DB_NAME"
DB_USER = "REDSHIFT_USER"
DB_PORT = "5439"
DB_PASSWORD = "REDSHIFT_PASSWORD"
BUCKET_NAME = "REDSHIFT_BUCKET_NAME"

s3 = boto3.resource('s3', 
                    region_name="us-east-1", 
                    aws_access_key_id=ACCESS_KEY, 
                    aws_secret_access_key=SECRET_KEY)

con = psycopg2.connect( dbname=DB_NAME, 
                        host="REDSHIFT_HOST_NAME",
                        port=DB_PORT, 
                        user=DB_USER, 
                        password=DB_PASSWORD)

bucket = s3.Bucket(BUCKET_NAME)

con.set_session(autocommit=True)
cur = con.cursor()


# open .csv files and load content
with open("student.csv", "r") as file:
    student_content = file.read()

with open("marks.csv", "r") as file:
    marks_content = file.read()

# load files to S3 bucket, require the user to have permission to do so
bucket.put_object(Body=student_content, Key="student.csv")
bucket.put_object(Body=marks_content, Key="marks.csv")

# create table over redshift cluster
cur.execute("CREATE TABLE IF NOT EXISTS STUDENT (NAME VARCHAR(20), ROLL INT, CLASS VARCHAR(20))")
cur.execute("CREATE TABLE IF NOT EXISTS MARKS (ROLL INT, TEST VARCHAR(20), MARKS INT)")

cur.execute("select * from student")
records = cur.fetchall()

# if first run
if len(records) == 0:
    # copy command to load data from s3 csv file to redshift
    cur.execute(""" COPY student 
                    FROM 's3://../student' 
                    iam_role 'arn:aws:iam:ROLE_DETAIL_HERE' 
                    delimiter ',' 
                    region 'us-east-1'
                    ignoreheader 1""")

cur.execute("select * from marks")
records = cur.fetchall()

if len(records) == 0:
    cur.execute(""" COPY marks 
                    FROM 's3://../marks' 
                    iam_role 'arn:aws:iam::ROLE_DETAIL_HERE' 
                    delimiter ',' 
                    region 'us-east-1'
                    ignoreheader 1""")

# run query with join to get data
cur.execute("select s.name, sum(m.marks) \
    from student s \
    join marks m \
    on s.roll=m.roll \
    group by s.name")
query_result = cur.fetchall()

# print item
for item in query_result:
    print(*item)