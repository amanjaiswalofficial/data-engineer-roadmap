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