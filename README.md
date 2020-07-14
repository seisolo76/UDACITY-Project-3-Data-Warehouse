# Project: Data Warehouse
## Purpose
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. I will be able to test the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.

## Schema
I used a star schema that is optimized for queries on song play analysis.

