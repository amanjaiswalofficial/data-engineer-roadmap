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