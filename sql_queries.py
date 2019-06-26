import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists fact_songplay;"
user_table_drop = "drop table if exists dim_user;"
song_table_drop = "drop table if exists dim_song;"
artist_table_drop = "drop table if exists dim_artist;"
time_table_drop = "drop table if exists dim_time;"

# CREATE TABLES

staging_events_table_create= ("""create table if not exists staging_events (artist varchar(200), auth varchar(20), firstName varchar(20), gender varchar(10), iteminSession integer, lastName varchar(20), length double precision, level varchar(10), location varchar(50), method varchar(10), page varchar(10), registration double precision, sessionId bigint, song varchar(50), status integer, ts bigint, userAgent text, userId int)
""")

staging_songs_table_create = ("""create table if not exists staging_songs (num_songs integer, artist_id varchar(20), artist_latitude double precision, artist_longitude double precision, artist_location varchar(50), artist_name varchar(50), song_id varchar(20), title varchar(50), duration double precision, year integer)
""")

user_table_create = ("""create table if not exists dim_user (user_id bigint primary key sortkey, first_name varchar(20) not null, last_name varchar(20) not null, gender varchar(1) not null, level varchar(10) not null) diststyle all""")

song_table_create = ("""create table if not exists dim_song (song_id varchar(50) primary key distkey sortkey, title varchar(50) not null, artist_id varchar(20) not null, year integer not null, duration decimal not null)""")

artist_table_create = ("""create table if not exists dim_artist (artist_id varchar(20) primary key sortkey, name varchar(50) not null, location varchar(50), latitude decimal, longitude decimal) diststyle all""")

time_table_create = ("""create table if not exists dim_time (start_time timestamp primary key sortkey, hour integer not null, day integer not null, week integer not null, month integer not null, year integer not null, weekday integer not null) diststyle all""")

songplay_table_create = ("""create table if not exists fact_songplay (songplay_id bigint identity(0,1) primary key, start_time timestamp references dim_time(start_time), user_id bigint references dim_user(user_id) sortkey, level varchar(10), song_id varchar(50) references dim_song(song_id) distkey, artist_id varchar(20) references dim_artist(artist_id), session_id bigint, location varchar(50), user_agent text)""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data' 
iam_role 'arn:aws:iam::729869189635:role/dwhRole'
compupdate off
format as json 's3://udacity-dend/log_json_path.json'
truncatecolumns;
""").format()

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data' 
iam_role 'arn:aws:iam::729869189635:role/dwhRole'
compupdate off
format as json 'auto'
truncatecolumns;
""").format()

# FINAL TABLES

songplay_table_insert = ("""insert into fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select
  timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
  e.userId as user_id,
  e.level as level,
  s.song_id as song_id,
  s.artist_id as artist_id,
  e.sessionId as session_id,
  e.location as location,
  e.userAgent as user_agent
from staging_events e
  join staging_songs s on s.artist_name = e.artist and e.song = s.title and e.length = s.duration
where
  e.page = 'NextSong'
  
""")

user_table_insert = ("""insert into dim_user (user_id, first_name, last_name, gender, level)
select 
  distinct userId as user_id,
  firstName as first_name,
  lastName as last_name,
  gender as gender,
  level as level
from staging_events
where
  page = 'NextSong';
""")

song_table_insert = ("""insert into dim_song (song_id, title, artist_id, year, duration)
select
  distinct song_id as song_id,
  title as title,
  artist_id as artist_id,
  year as year,
  duration as duration
from staging_songs;
""")

artist_table_insert = ("""insert into dim_artist (artist_id, name, location, latitude, longitude)
select 
  distinct artist_id as artist_id,
  artist_name as name,
  artist_location as location, 
  artist_latitude as latitude,
  artist_longitude as longitude
from staging_songs;
""")

time_table_insert = ("""insert into dim_time (start_time, hour, day, week, month, year, weekday)
select
  distinct timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
  extract(hour from (timestamp 'epoch' + ts/1000 * interval '1 second')) as hour,
  extract(day from (timestamp 'epoch' + ts/1000 * interval '1 second')) as day,
  extract(week from (timestamp 'epoch' + ts/1000 * interval '1 second')) as week,
  extract(month from (timestamp 'epoch' + ts/1000 * interval '1 second')) as month,
  extract(year from (timestamp 'epoch' + ts/1000 * interval '1 second')) as year,
  extract(weekday from (timestamp 'epoch' + ts/1000 * interval '1 second')) as weekday
from staging_events
where
  page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
