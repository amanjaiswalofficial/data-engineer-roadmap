## Notes from Data Warehousing Fundamentals for Beginners

### <u>DWH Concepts</u>

#### DWH are

- Integrated - *Data from n no. of sources*
- Subject oriented
- Time variant - *contains historical data as well*
- Non-volatile - *periodic update only, allows the analytics to plan strategically*

#### Why DWH ?

- Make data driven decisions (views of past/present/future if possible) - lead to BI
- One-stop shopping (due to integrated nature of DWH)

#### End to end workflow of DWH

Multiple Data Sources >>>> **ETL** >>>> DWH >>>> (can move even further) **ETL** >>>> Data Marts (smaller)

**Example analogy -** 

Supplier >>>> **ETL** >>>> Wholesaler >>>> **ETL** >>>> Retailer (specific items/smaller subsets)



### <u>DWH Architecture</u>

#### Different type of data marts

- **Dependent data marts** 

  *depend on DWH and extracting data from 'em via ETL to make smaller use case specific data sets.*

- **Independent data marts** 

  *depend on the sources itself to ETL data by themselves to create data mart*

- Major differences
  - sourced from DWH **vs** sources from application systems
  - uniform data across marts **vs** little uniformity in data since different sources
  - straightforward in arch. **vs** can be in a web like arch.

Single major difference b/w DWH and an independent data mart (as both draw data from sources itself) is that independent data mart might be from lesser number of sources, whereas DWH is guaranteed to be from dozen of different type of sources. 

#### Cubes

- Not a RDBMS
- Instead, more like a dimensionally aware database
- More suited for small scale DW

Pros & Cons of Cubes

- Faster response time
- Work on small data volumes (100GBs)
- Less flexible, and more rigid compared to a RDBMS



Same analogy can be written as, while using cubes 

Data Sources >>>> **ETL** >>>> DWH >>>> **ETL** >>>> [Cubes] being used as data marts



#### ODS - *Operational Data Store*

- Similar to DWH, as integrates data from several sources
- But different in the sense, that it focuses more on current operational data
- Hence, often, real-time source are used for ODS to get data in that manner instead of a batch or period wise update as done in DWH



#### Staging Layer

DWH = Staging + User Access area

**Importance of Staging Layer**

- Hence, Staging Layer is more of a landing zone, where data from several sources come from, and sits, which is then passed on to the User Access area, this UAa is what appears to be the DWH for the end users.
- Analogy hence can be re-written as

Data Sources >>>> ETL >>>> [                          DWH                           ] >>>> ETL >>>> ....

Data Sources >>>> ETL >>>> [Staging Layer >> User Access Layer] >>>> ETL >>>> ....

- Staging Layer hence is focused on E of ETL majorly

**Types of Staging Layer**

- Non Persistent & Persistent
- Data once loaded to UAL deleted from SL **vs** Data kept at SL even after loading to UAL
- Less storage space **vs** more storage space
- Data QA requires to verify with source **vs** no such requirement as all data still in SL
- Require to use source again to rebuild UAL if needed in this case **vs** data from SL can be used to rebuild UAL



### <u>ETL</u>

##### Extract

- To quickly pull data from sources, usually done in batches
- Will probably include errors as its purely raw data from sources
- Lands in DWH SL

##### Transform

- Compare the data from different sources
- Prepare the data as uniform as possible for UAL

##### Load

- Post transform, must load data to UAL, to be used for BI and analytics.

Challenges with ETL include

- some amount of data modelling and analytics before initializing the ETL itself



#### Types of ETL

- Initial - *one time, full relevant data load from source, require redo in case of blow up*

- Incremental - *refresh DWH periodically, Bring new data, update existing data, might delete data i.e. set some records to inactive to match source* - in a nutshell - **bring DWH up to date with source's data**

  Incremental patterns

  - Append
  - In-place update (same no. of rows, only updated)

#### Role of transformation

- To achieve: **uniformity** in data in DWH that's from different source
- To do **restructuring** of the data if needed

Major models of Transformation

- **Data value unification** - unify data in similar cols from different sources to have similar looking data for that column when reaching in UAL
- **Data type/size unification** - similar to 1, but applies to size and type of rows of data
- **De-duplication** - if several source contain same data, get rid of duplicity and keep only distinct data
- **Vertica slicing/drop cols**. -  Drop columns that aren't needed for UAL
- **Horizontal slicing/drop rows** - Drop rows that aren't needed any more for this UAL
- **Correcting known errors** - Fix known errors at this step before loading to UAL



### <u>Dimensionality</u>

A data-driven decision (one end goal of DWH) = A measurement + Some related dimension context

A dimension context can be provided to any measurement:

- Using **By**, to group data for entire dimension
- Using **For**, to find out even more specific data out of that dimension



Examples:

- 2000 Rupees (Measurement) - Salary of a person grouped by month (Dimension context)
- Questions including By/For
  - Avg annual salary by rank, by dept., by year [Using By]
  - Avg annual salary for a professor by rank, by dept., by year [Using By & For]

#### Facts & Dimensions

Based on above examples

**A measurement is generally a fact** - *measurable, metric, numeric*

A fact is not always same as a fact table i.e. not everything that is a fact can be converted into fact table data. 

**Ex -** Doing something is a fact, but not a measurable or numeric thing to be converted into fact table

**Ex -** Salary, Credit Hours etc

**The context for a fact is a dimension**

**Ex -** Dimension is interchangeable with dimension table

**Ex -** Employee, Major, Department etc

**Different type of additivity for fact data**

1. Additive, *something that can be added at all times*, 

Ex - salaries of employee, no. of credit hours

2. Non-additive, *something that can't be added*, 

Ex - percentage of student marks, cant be added to each other and give valid data

3. Semi additive fact - ?

#### Star vs Snowflake - Part 1

BI <- OLAP <- Dimensionality <- dim & fact tables <- constructed as either star or snowflake schema

| Star Schema                                           | Snowflake Schema                                     |
| ----------------------------------------------------- | ---------------------------------------------------- |
| All dimensions of any hierarchy are at a single level | each dimension of hierarchy is in its own table      |
| Always 1 level away from fact table                   | 1or 2 level away from fact tables                    |
| Fewer db joins to drill up/down data                  | more joins required for the same                     |
| Primary -> Foreign key relationships simpler          | Primary -> Foreign key relationships complex         |
| Denormalized dimension tables                         | Normalized dimension tables due to breakdown of data |

Both have same dimensions, only different table representations.



#### Keys for DWH

- Primary Keys, *unique identifiers*
- Foreign Key, *unique identifier from other tables*
- Natural Keys, *may be cryptic or readable, usually travel from source itself to the DWH, like some kind of ID*
- Surrogate Keys, *generated by database itself, having no business meaning but can be used for unique identification*



#### Star vs Snowflake - Part 2

**Faculty -> Department -> College**, in **star** can be written as 

[ **faculty_key**, **faculty_id**, **f_name**, **year_**....., **dept_id**, **dep_name**....., **college_id**, **college_name** ]

**Important observations**:

- Faculty_key <- surrogate key, made by DB itself to uniquely identify rows, hence same primary key too

- Faculty_id, dept_id, college_id <- natural keys, came with data from source itself

- All such data in a single row, i.e. single dimension table for complete hierarchy



**Faculty -> Department -> College**, in **snowflake** can be written as

[ **faculty_key**, **faculty_id**, f_name, year... **dept_key** [FK] ]

[ **dept_key**, **dept_id**, dep_name... **college_key** [FK] ]

[ **college_key**, **college_id**, college_name... ]

**Important observations:**

- Every hierarchy in a separate dimension table

- Each non-terminal table (*something that has a parent in hierarchy*), has a surrogate key/primary key.

- Each child non-terminal table uses this primary key as FK to identify data in its parent table

- Terminal table, has no FK

  

### <u>Fact Tables - Theory</u>

Discussed various type of fact tables

#### 1.Transaction fact tables

- Store measurements and similar metrics from data

- Example:

  | tuition payment (a fact) | student_key (FK/surrogate key from student table) | date_key (FK/surrogate key from date table) ?? |
  | ------------------------ | ------------------------------------------------- | ---------------------------------------------- |
  | 1000 Rupees              | used instead of student name or anything as such  | same as student                                |

- 2 major rules

  - Both facts should be available at same level
  - Facts should occur simultaneously

  As long as both these rules apply, as many as needed, facts can be combined together

  Examples:

  1. **BAD**: [tuition bill & tuition payment] (*both dont occur simultaneously*)

  2. **GOOD**: [tuition billed amount & activity billed amount] (same level as well as may be simultaneous)



#### 2. Periodic Snapshot fact tables

- Record <u>**regular/periodic measurements a**</u>s periodic snapshot of something

- Includes - transactions that can be **some sort of aggregation** over regular transaction as well as those that aren't directly an aggregation

- Example

  1. GOOD:  Canteen Balance tracking

     | Student Key (surrogate key for student's table) | Week Key (another key from different table) | EOW Balance |
     | ----------------------------------------------- | ------------------------------------------- | ----------- |
     | ABCDE                                           | 121                                         | 5000        |

- Snapshot fact tables data can be **semi-additive** as well i.e. sometimes it can be added, sometimes it can't be.
  - Example of such facts - 
    - Account balance from a student from above table can't be added to a sum balance
    - But if we lock one of the cols, the average bal. for a specific student over time can be measured.
    - Similarly, avg of different students can be calculated and compared

#### 3. Accumulating Snapshot fact tables

- Such facts can be used to <u>**measure things like time spent in different phases**</u>

- May include items that are completed as well as items that are still in progress

- Introduces **concept of 1-to-many relationship** between 1 dim and 1 fact table

- Example: **Student application processing**

  | S_Key | Application Submission Date (Date key) | Application 1st processing date (Date key) | Application 2nd processing date (Date key) | Application Submitted By (Employee key) | 1st processing done by (Employee key) | 2nd processing done by (Employee key) |
  | ----- | -------------------------------------- | ------------------------------------------ | ------------------------------------------ | --------------------------------------- | ------------------------------------- | ------------------------------------- |
  | 121   | 01062021                               | 10062021                                   | 2002021-                                   | 1ABC                                    | 2BCD                                  | 3DEF                                  |
  |       |                                        |                                            |                                            |                                         |                                       |                                       |

  Here 3 type of dimensions are used:

  1. **Student**, whose S_Key is used
  2. **Date**, whose Date key is used in several columns, hence 1-to-many relationship
  3. **Employee**, who process application, identified by Employee key, in several cols (1-to-many)

- This can include more columns, like total days taken for submission, days for 1st processing etc, which will increment based on the dates in the first columns

- Such type of data can be put under category of Accumulating Snapshot tables

#### 4. Factless fact tables

- To <u>**record a transaction that has no measurements**</u>

- Whenever <u>**an event occurs**</u>, we want to **<u>track</u>**, but since there's no such significant metric, we only record its measurement in some way.

- Example: Student webinar registration

  | Student Key | Date Registration Key | Date Scheduled Key | Webinar Key |
  | ----------- | --------------------- | ------------------ | ----------- |
  | A123        | 12291900              | 12928492           | ABC         |

  Here, 3 type of dimensions are used:

  1. **Student**: to link the student who registered for the webinar
  2. **Date**: to track the date when this registration happened, also when this webinar was scheduled, so can also be a 1-to-many kinda relationship
  3. **Webinar**: to track the webinar and link with registration

  There are **<u>no other columns</u>** in this table <u>except keys to other tables</u>, hence a **factless table** since only used to record a transaction/operation/occurrence

  Can be used for operations like sum(), count() or queries that have some kind of filter over a specific column

  **Alternative**: **<u>Tracking fact</u>**, where a dummy column, with value 1 is also put along with all data, which can be used in operations like sum() later

- A <u>**variant**</u> to the above factless table can be for instances <u>where an event with starting_date and ending_date may occur</u>, so both keys can be part of such fact table

- Example: Student mentor assignment

| Student Key | Mentor Assigned Date | Mentor Removed Date | Mentor Key |
| ----------- | -------------------- | ------------------- | ---------- |
|             |                      |                     |            |

### <u>Star vs Snowflake - Fact + Dim</u>

RECAP:

For dim tables: [**Faculty**, **Department**, **College**]

#### In a star schema:

| Faculty Key (PK) | Faculty ID | F_Name | Dept_ID | Dept_Name | College_ID | College_name | Year |
| ---------------- | ---------- | ------ | ------- | --------- | ---------- | ------------ | ---- |
|                  |            |        |         |           |            |              |      |

#### In a snowflake schema

| Faculty_Key | Faculty_ID | F_Name | Dept_Key (FK) |
| ----------- | ---------- | ------ | ------------- |
|             |            |        |               |

| Dept_Key (PK)/(FK) | Dept_ID | Dept Name | College Key (FK) |
| ------------------ | ------- | --------- | ---------------- |
|                    |         |           |                  |

| College Key (PK)/(FK) | College ID | College Name | Year |
| --------------------- | ---------- | ------------ | ---- |
|                       |            |              |      |

Comparison Notes

1. In *star schema*, all the 3 levels of hierarchy are in a single dim table **VS** 

   In <u>snowflake schema</u>, 3 different tables

2. *Star schema* uses a single PK and no FK to any table **VS** 

   In <u>snowflake schema</u>, faculty uses a FK to Department and Department uses FK to College. College being the topmost level, have no FK to any other table.

3. In *Star schema*, all relevant information is at a single level **VS** 

   In <u>snowflake schema,</u> it would require several joins to get all relevant information in case of some queries



In an environment having fact tables as well

#### In a star schema

| Fact_1 | Fact_2 | Faculty_Key (FK) | Some_Other_table_FK |
| ------ | ------ | ---------------- | ------------------- |
|        |        |                  |                     |

Having a dim table as

| Faculty Key (PK) | Faculty ID | F_Name | Dept_ID | Dept_Name | College_ID | College_name | Year |
| ---------------- | ---------- | ------ | ------- | --------- | ---------- | ------------ | ---- |
|                  |            |        |         |           |            |              |      |



#### In a snowflake schema

For a snowflake schema for similar fact table as above, instead of storing FKs to all tables in the hierarchy (College PK, Department PK, Faculty PK) -> We can leverage the relationship between them and only use the FK to the lowest level of this hierarchy 

i.e

Fact Table only has FK to Faculty

Faculty can give FK to Department

Department can give FK to College

| Fact_1 | Fact_2 | Faculty_Key (FK) | ...  |
| ------ | ------ | ---------------- | ---- |
|        |        |                  |      |

| Faculty_Key (PK)/(FK) | Faculty_ID | F_Name | Dept_Key (FK) |
| --------------------- | ---------- | ------ | ------------- |
|                       |            |        |               |

| Dept_Key (PK)/(FK) | Dept_ID | Dept Name | College Key (FK) |
| ------------------ | ------- | --------- | ---------------- |
|                    |         |           |                  |

| College Key (PK)/(FK) | College ID | College Name | Year |
| --------------------- | ---------- | ------------ | ---- |
|                       |            |              |      |

**Notes: On SQL for fact tables**

 -- PK for transaction fact tables will consist of keys of multiple tables (generally)



### <u>SCD - Slowly Changing Dimensions</u>

**Requirement**:
As time passes, data in the tables may change over time, to adapt to changing data. SCD is used
It updates the database to the recent version of the data based on the type used on it, to ensure the latest data (and/or the previous history of it) is available for analytics and reference.

#### Type - 1 SCD

- In simple words, rewrite the data

- **Technique**: In-place update

- **Impact**: No history maintenance

- **Application**: To correct incorrect values in the database, by updating and getting rid of old values

- **Example**: Correcting birth year in the records

  | Student_Key | Student_ID | Birthdate |
  | ----------- | ---------- | --------- |
  | ABC         | 123        | 01/03/19  |

  After applying SCD-1, and updating the specific col's data for specific row

  | Student_Key | Student_ID | Birthdate  |
  | ----------- | ---------- | ---------- |
  | ABC         | 123        | 01/03/2000 |

- **Advantage and Disadvantages**
  - Simplest to use
  - Used to remove errors in the data
  - Especially useful when no need to track history
  - As a side effect, analytics may get impacted due to deleted data. Also, it may require to track errors but since they aren't kept, can't do so in case of SCD-1.

#### Type - 2 SCD

- In simple words, update the row

- **Technique**: The existing row remains as is. Instead a new row is added with similar data as the old one with changes to the columns that occurred which required SCD. Afterwards, the latest row for the previously stored record contains the latest data for this.

- **Impact**: Might rise some complication with reporting/analytics for future records

- **Application**: Ensure all version of the data exist in the database, hence previous analytics will never be impacted

- **Example**: Correcting the birth year

  | Student_Key | Student_ID | Birthdate  |
  | ----------- | ---------- | ---------- |
  | ABC         | 123        | 01/03/1990 |

  After applying SCD-2, and creating a new row with correct data

  | Student_Key | Student_ID | Birthdate  |
  | ----------- | ---------- | ---------- |
  | ABC         | 123        | 01/03/1990 |
  | CDE         | 123        | 01/03/2000 |

  As visible, the new record has same **Student_ID** (Natural key, fetched from source), but new **Student_Key** (Primary key, generated at the database level itself), to ensure a unique identifier for the new row.

- **Advantages & Disadvantages**

  - Since all data is always present, irrespective of complications - can be referred for unlimited history.
  - As a side effect, its much more complex to implement compared to other types

<u>Complications in case of Fact tables</u>

In order to relate the fact of this student, Example: FACT_MARKS

| Student_Key | Student_ID | Marks_In_English |
| ----------- | ---------- | ---------------- |
| ABC         | 123        | 89               |

As the dim_table is updated, same can be done with fact for this student

| Student_Key | Student_ID | Marks_In_English |
| ----------- | ---------- | ---------------- |
| ABC         | 123        | 89               |
| CDE         | 123        | 89               |

But, to ensure, this is related to the latest and correct row with Student_ID (123), 2 type of solutions are present

1. Add a flag in dim table, so that previous rows can be flagged to inactive or N in the database

   **Before**

   | Student_Key | Student_ID | Birthdate  | Active |
   | ----------- | ---------- | ---------- | ------ |
   | ABC         | 123        | 01/03/1990 | Y      |

   **After**

   | Student_Key | Student_ID | Birthdate  | Active |
   | ----------- | ---------- | ---------- | ------ |
   | ABC         | 123        | 01/03/1990 | N      |
   | CDE         | 123        | 01/03/2000 | Y      |

   This way, using active flag, it can always be used to ensure that the join with fact or FK from fact points to the most correct row for this **Student_ID**

   But even this can become problematic, as multiple updates come for same Student_ID, will result in multiple rows with Active=N, hence not possible to track which was active until when (No time period tracking)

2. As a fix, 2nd solution i.e. Using columns like **effective_date** and **expiry_date** along with data to know which row was the latest until when

   **Before**

   | Student_Key | Student_ID | Birthdate  | effective_date | expiry_date |
   | ----------- | ---------- | ---------- | -------------- | ----------- |
   | ABC         | 123        | 01/03/1990 | 01/01/2021     | 31/12/2030  |

   **After**

   | Student_Key | Student_ID | Birthdate  | effective_date | expiry_date |
   | ----------- | ---------- | ---------- | -------------- | ----------- |
   | ABC         | 123        | 01/03/1990 | 01/01/2021     | 31/03/2021  |
   | CDE         | 123        | 01/03/2000 | 01/04/2021     | 31/12/2030  |

   This way, using the 2 columns, it can be used to check which row was active in what time period.

3. A 3rd variant is a combination of using both **Active** and **effective_date/expiry_date**



#### Type - 3 SCD

- In simple words, update the columns

- **Technique**: Add new columns as new data comes to keep track of data (very limited use-cases)

- **Impact**: Allows to easily switch back between past and present reporting of a column's data

- **Application**: Allows flexible reporting for past and present data based on columns. Hence, useful for reorganizing of data, while keeping the older data as well in the same database

- **Example**: Changing of some column's data permanently in the future. Like, changing marks from integers to grades 

  Previously

  | Student_Key | Student_ID | Marking |
  | ----------- | ---------- | ------- |
  | ABC         | 123        | 79      |

  Now, as the marking changed, simply a new column can be added and old column's name can be updated to

  | Student_Key | Student_ID | New_Marking | Old_Marking |
  | ----------- | ---------- | ----------- | ----------- |
  | ABC         | 123        | B           | 79          |

- **Advantages & Disadvantages**

  - Easy to use compared to SCD-2
  - Can have more than 2 columns like that depending on use cases. Example:
    **1st_Marking**: 1-100 (int)
    **2nd_Marking**: A-F (grades)
    **3rd_Marking**: Group A, Group B, Group C (groups)
  - As <u>a side effect</u>, can't add too many changes like this. In that case, prefer using SCD-2. 
  - Also, only applicable when such a change is for all the rows in the database and not specific to some rows.



### <u>ETL Design Guidelines</u>

#### Basic Tips

1. Limit the data that ETL brings from source to DWH. Tips include:
   1. Using incremental approach
   2. Checking the last date when the ETL was successful and only bring data that's post that date, i.e. having a timestamp like column over source at least to identify records post a specific date
   3. Other ways include: Using scan and compare between the 2 tables i.e. source and DWH's table
2. Process dim tables before fact tables
3. Find whenever parallel processing can be utilized

#### Steps For ETL

1. Data preparation [*getting only as much as data as required, i.e. incremental/compared*]

2. Data transformation [*Using 1 of the 6 transformation techniques: De-duplication/unification etc*]

3. Process new dimension rows [*Append*]

4. Process SCD type-1 rows [*Data that require correction, or* *in-place update*] (DIM TABLES)

5. Process SCD type-2 rows [*Append data with new surrogate keys*] (DIM TABLES)

6. While updating fact tables, the dim data the fact table refer to may be multiple (post scd-2, there can be multiple rows for same ID (*natural key of source data*)).

    Hence every new fact row should have

   1. The fact data
   2. The ID value for the dim table
   3. A col with date value to identify in which dim row's effective_date/expiry_date it occurs with

**Question**:  Why SCD type-1 before SCD type-2?

### <u>Cloud Data Warehousing</u>

Major **advantages** of using DWH over cloud

- Lower platform investment
- Disaster recovery
- Easier sync with cloud data lakes

Major **challenges** with Cloud Data WH

- Limited security control
- Migration from 1 type of cloud to another
- Migration of existing on-premise DWH to cloud DWH



## Think Dimensional