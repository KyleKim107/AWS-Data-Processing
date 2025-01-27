# AWS-Data-Processing
This repository contains the implementation of a scalable and robust data processing pipeline designed to extract, transform, and load (ETL) data from public data portals into structured, optimized formats for analysis and reporting. The project leverages modern cloud technologies and big data tools for efficient processing.


### Overview

![](images/overview.png)
### The data processing pipeline follows a three-tier architecture:
- Bronze Data: Raw data containing duplicates, errors, or irrelevant information.
- Silver Data: Cleaned and transformed data, refined but not yet fully optimized for analysis.
- Gold Data: Fully processed and business-ready data for analytics, reporting, and KPI generation.

### Part.1

![](images/part1.png)

#### Ingestion Phase

**- Data Source:** Real estate transaction data is fetched from the public data portal at regular intervals.  
**- AWS Lambda:** A Lambda function automates the data ingestion process, triggered at scheduled intervals to fetch data.

**Creating Bronze Data**

**- RDS Storage:** The fetched data is stored in a relational database (RDS) in a structured format to maintain the initial database schema.  
**- S3 Storage:** Simultaneously, the data is saved in JSON format in an S3 bucket for raw data preservation.  
    - This unprocessed data, referred to as Bronze Data, may include duplicates, errors, or irrelevant information.

### Part.2

![](images/part2.png)
