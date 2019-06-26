# Details for Project Data Warehouse

## Goal - Automated data pipeline that extracts songplay and song data from s3 and creates tables in Redshift for analysis

## Database design - combination of dimensional and fact tables.  Three dimensional tables are distributed across all slices due to smaller size and are sorted by primary key.  Fact table references the primary key of each of the dimension tables

## aws_setup.py
This file was used to create and delete a redshift cluster.  Normally a redshift cluster would be created prior to this implementation and be running at all times

## create_tables.py
Runs sql located in sql_queries.py (see below)

## sql_queries.py
Drops tables if they exist

Create tables
- staging tables
staging_events and staging_songs - tables used to copy raw data from s3 into staging tables used as the source of the official dimensional and fact tables
- dimensional tables
dim_user - user_id primary and sort key, table distributed across all slices due to limited size
dim_song - song_id primary key and distribution key for connection with fact_songplay 
dim_artist - artist_id primary and sort key, table distributed across all slices due to limited size
dim_time - start_time primary and sort key, table distributed across all slices due to limited size
- fact table
fact_songplay - fact table which references all dimensional tables and creates a songplay id based on incremental count

Staging tables - copy data from s3 to staging tables, staging events format references a specific format

Insert tables
- dim_user - created from staging_events table, filtered on NextSong, unique users only
- dim_song - created from staging_songs, unique songs only
- dim_artist - created from staging_songs, unique artists only
- dim_time - created from staging_events, unique start times only, date and time components extracted
- fact_songplay - fact table created from staging_events and staging_songs, provides detail on each song play from the artist, song, and user perspective

## Sample queries
Top song plays by artist name
select
  name as artist_name,
  count(songplay_id) as song_count
from public."fact_songplay" f
  join public."dim_artist" a on a.artist_id = f.artist_id
group by 1
order by 2 desc
limit 10;

*Results
artist_name,song_count
Dwight Yoakam,37
Kid Cudi / Kanye West / Common,10
Ron Carter,9
Lonnie Gordon,9
B.o.B,8
Usher featuring Jermaine Dupri,6
Muse,6
Richard Hawley And Death Ramps_ Arctic Monkeys,5
Counting Crows,4
Metallica,4*

Song play counts by day
select
  day as day,
  count(songplay_id) as song_count
from public."fact_songplay" f
  join public."dim_time" a on a.start_time = f.start_time
group by 1
order by 2 desc;

*Results
day,song_count
15,25
5,25
29,23
21,18
26,18
28,17
13,17
30,16
14,15
24,14
9,13
20,11
23,11
19,10
27,9
8,8
7,8
4,8
17,7
16,7
12,6
22,6
6,5
10,5
3,4
2,4
18,3
11,3
25,2
1,1
*




