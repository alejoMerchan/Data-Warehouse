# Project: Data Warehouse

The idea of this project is to build a data warehouse using AMAZON S3 and AMAZON Redshift.


## High Level Functionality

We are going to generate a data model in Redshift, that model is created reading the principal information from S3 and inserting that data in a staging table.
Once we have all the relevant information, we are going to charge the data to Redshift.

## Prerequisites

1. Redshift cluster created in AWS
2. Python 3.x

## Deployment

1. Redshift cluster configurations in dwh.cfg 
2. Execute "create_tables.py"
3. Execute "etl.py"
