# Project: Data Warehouse
## Purpose
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  

I was tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. I will be able to test the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.  

## Schema
I used a star schema that is optimized for queries on song play analysis.  
The Fact Table is songplay_table it has six of the eight columns are from staging_events data (start_time, userId, level, sessionId, location, userAgent) and two from staging_songs (song_id, artist_id).  
The Dimension Tables are as follows:  
users - _users in the app_ (user_id, first_name, last_name, gender, level)   
songs - _songs in the music database_ (song_id, title, artist_id, year, duration)  
artists - _artist in the music database_ (artist_id, name, location, lattitude, longitude)  
time - _timestamps of records in songplays broken into units in sperate columns_ (start_time, hour, day, week, month, year, weekday)  


![Project 3 Schema](https://github.com/seisolo76/UDACITY-Project-3-Data-Warehouse/blob/master/Project%203%20schema.png)


