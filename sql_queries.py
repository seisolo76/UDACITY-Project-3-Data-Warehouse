import configparser


# reads the configuration file that includes cluster, iam user, s3 bucket location
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES if they exsist

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES 

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table
(
  event_id                INTEGER IDENTITY(0,1) PRIMARY KEY,
  artist_name              VARCHAR(255),
  auth                     VARCHAR(50),
  first_name               VARCHAR(255),
  gender                   VARCHAR(1),
  itemInSession            INTEGER,
  last_name                VARCHAR(255),
  length                   FLOAT8,
  level                    VARCHAR(50),
  location                 VARCHAR(255),
  method                   VARCHAR(25),
  page                     VARCHAR(25),
  registration             BIGINT,
  sessionId                BIGINT,
  song                     VARCHAR(255),
  status                   INTEGER,
  ts                       BIGINT,
  userAgent                VARCHAR(255),
  userId                   INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table
(
  song_id                  VARCHAR(100),
    num_songs              INTEGER,
    artist_id              VARCHAR(100),
    artist_latitude        DOUBLE PRECISION,
    artist_longitude       DOUBLE PRECISION,
    artist_location        VARCHAR(255),
    artist_name            VARCHAR(255),
    title                  VARCHAR(255),
    duration               DOUBLE PRECISION,
    year                   INTEGER,
    PRIMARY KEY (song_id)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table
(
  songplay_id            INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
  start_time             BIGINT NOT NULL,
  user_id                BIGINT NOT NULL,
  level                  VARCHAR(50) NOT NULL,
  song_id                VARCHAR(100) NOT NULL,
  artist_id              VARCHAR(100) NOT NULL,
  session_id             BIGINT,
  location               VARCHAR(255),
  user_agent             VARCHAR(255)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table 
(
  user_id               INTEGER NOT NULL PRIMARY KEY,
  first_name            VARCHAR(255),
  last_name             VARCHAR(255),
  gender                VARCHAR(1),
  level                 VARCHAR(50)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table
(
  song_id              VARCHAR(100) NOT NULL PRIMARY KEY,
  title                VARCHAR(255),
  artist_id            VARCHAR(100),
  year                 INTEGER,
  duration             DOUBLE PRECISION
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table
(
  artist_id            VARCHAR(18) NOT NULL PRIMARY KEY,
  name                 VARCHAR(255),
  location             VARCHAR(255),
  latitude             DOUBLE PRECISION,
  longitude            DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table
(
  start_time          TIMESTAMP,
  hour                INTEGER NOT NULL,
  day                 INTEGER NOT NULL,
  week                INTEGER NOT NULL,
  month               INTEGER NOT NULL,
  year                INTEGER NOT NULL,
  weekday             VARCHAR(9)
);
""")

# load STAGING TABLES from s3 buckets

staging_events_copy = ("""
copy staging_events_table from '{}' 
credentials 'aws_iam_role={}'
region 'us-west-2'
JSON '{}';
""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs_table from '{}'
credentials 'aws_iam_role={}'
region 'us-west-2'
JSON 'auto';
""").format(config.get('S3','SONG_DATA'),
                        config.get('IAM_ROLE', 'ARN'))

# load FINAL TABLES from staging tables

songplay_table_insert = ("""INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    FROM staging_events_table e, staging_songs_table s
    WHERE e.page = 'NextSong'
    AND e.song = s.title
""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userid, first_name, last_name, gender, level
    FROM staging_events_table
    WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs_table
""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs_table
""")

time_table_insert = ("""INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day, EXTRACT(w from start_time) AS week, EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, EXTRACT(weekday from start_time) AS weekday 
    FROM staging_events_table
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
