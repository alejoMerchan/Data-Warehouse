import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS  staging_events(
artist VARCHAR(255),
auth VARCHAR(255),
firstName VARCHAR(255),
gender CHAR,
itemInSession INT,
lastName VARCHAR(255),
length DOUBLE,
level VARCHAR(255),
location VARCHAR(255),
method VARCHAR(255),
page VARCHAR(255),
registration FLOAT,
sessionId INT,
song VARCHAR(255),
status INT,
ts BIGINT,
userAgent VARCHAR(255),
userId INT);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(
num_songs INT,
artist_id VARCHAR(255),
artist_latitude DOUBLE,
artist_longitude DOUBLE,
artist_location VARCHAR(255),
artist_name VARCHAR(255),
song_id VARCHAR(255),
title VARCHAR(255),
duration DOUBLE,
year INT);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay(
songplay_id INT IDENTITY (0,1) PRIMARY KEY,
start_time TIMESTAMP,
user_id INT,
level VARCHAR(100),
song_id VARCHAR(150),
artist_id VARCHAR(150),
session_id INT,
location VARCHAR(225),
user_agent VARCHAR(225));
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS user(
user_id INT PRIMARY KEY,
first_name VARCHAR(225),
last_name VARCHAR(225),
gender CHAR,
level VARCHAR(100));
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS song(
song_id VARCHAR(150) PRIMARY KEY,
title VARCHAR(225),
artist_id VARCHAR(150),
year INT,
duration FLOAT);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist(
artist_id VARCHAR(150) PRIMARY KEY,
name VARCHAR(225),
location VARCHAR(225),
latitude DOUBLE,
longitude DOUBLE);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} iam_role {} FORMAT AS JSON 'auto' COMPUPDATE OFF REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
COPY staging_songs FROM {} iam_role {} FORMAT AS JSON 'auto' COMPUPDATE OFF REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(select timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time, e.userId, e.level, s.song_id, s.artist_id, e.sessionid, e.location, e.userAgent FROM staging_events AS e LEFT OUTER JOIN staging_songs AS s ON e.song = s.title)
""")

user_table_insert = ("""
INSERT INTO user(user_id, first_name, last_name, gender, level)
(SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events WHERE userId IS NOT NULL)
""")

song_table_insert = ("""
INSERT INTO song(song_id, title, artist_id, year, duration)
(SELECT DISTINCT song_id, title, artist_id, year, duration from staging_songs)
""")

artist_table_insert = ("""
INSERT INTO artist(artist_id, name, location, latitude, longitude)
(SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs)
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
(SELECT start_time, EXTRACT(hr from start_time) AS hour, EXTRACT(d from start_time) AS day, EXTRACT(w from start_time) AS week, EXTRACT(mon from start_time) AS month, EXTRACT(yr from start_time) AS year, EXTRACT(weekday from start_time) AS weekday FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time FROM staging_events s) WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
