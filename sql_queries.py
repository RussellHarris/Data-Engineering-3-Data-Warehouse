import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist_name     VARCHAR          NULL,
    auth            VARCHAR          NOT NULL,
    first_name      VARCHAR          NULL,
    gender          CHAR(1)          NULL,
    item_in_session SMALLINT         NOT NULL,
    last_name       VARCHAR          NULL,
    length          DOUBLE PRECISION NULL,
    level           VARCHAR          NOT NULL,
    location        VARCHAR          NULL,
    method          VARCHAR          NOT NULL,
    page            VARCHAR          NOT NULL,
    registration    BIGINT           NULL,
    session_id      SMALLINT         NOT NULL,
    song            VARCHAR          NULL,
    status          SMALLINT         NOT NULL,
    ts              TIMESTAMP        NOT NULL DISTKEY SORTKEY,
    user_agent      VARCHAR          NULL,
    user_id         SMALLINT         NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs        SMALLINT         NOT NULL,
    artist_id        VARCHAR          NOT NULL,
    artist_latitude  DOUBLE PRECISION NULL,
    artist_longitude DOUBLE PRECISION NULL,
    artist_location  VARCHAR          NULL,
    artist_name      VARCHAR          NOT NULL,
    song_id          VARCHAR          NOT NULL DISTKEY SORTKEY,
    title            VARCHAR          NOT NULL,
    duration         DOUBLE PRECISION NOT NULL,
    year             SMALLINT         NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    start_time  TIMESTAMP             NOT NULL REFERENCES time(start_time),
    user_id     SMALLINT              NOT NULL REFERENCES users(user_id),
    level       VARCHAR               NOT NULL, 
    song_id     VARCHAR               NULL REFERENCES songs(song_id),
    artist_id   VARCHAR               NULL REFERENCES artists(artist_id),
    session_id  SMALLINT              NOT NULL,
    location    VARCHAR               NOT NULL,
    user_agent  VARCHAR               NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id    SMALLINT NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    first_name VARCHAR  NOT NULL,
    last_name  VARCHAR  NOT NULL,
    gender     CHAR(1)  NOT NULL,
    level      VARCHAR  NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id   VARCHAR          NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    title     VARCHAR          NOT NULL,
    artist_id VARCHAR          NOT NULL,
    year      SMALLINT         NOT NULL,
    duration  DOUBLE PRECISION NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR          NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    name      VARCHAR          NOT NULL,
    location  VARCHAR          NULL,
    lattitude DOUBLE PRECISION NULL,
    longitude DOUBLE PRECISION NULL
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    hour       SMALLINT  NOT NULL,
    day        SMALLINT  NOT NULL,
    week       SMALLINT  NOT NULL,
    month      SMALLINT  NOT NULL,
    year       SMALLINT  NOT NULL,
    weekday    SMALLINT  NOT NULL
);
""")

# STAGING TABLES
#https://www.flydata.com/blog/how-to-improve-performance-upsert-amazon-redshift/
#https://www.intermix.io/blog/improve-redshift-copy-performance/
staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
JSON {}
TIMEFORMAT 'epochmillisecs'
COMPUPDATE OFF STATUPDATE OFF;
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
JSON 'auto'
COMPUPDATE OFF STATUPDATE OFF;
""").format(config.get('S3','song_data'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts
     , e.user_id
     , e.level
     , s.song_id
     , s.artist_id
     , e.session_id
     , e.location
     , e.user_agent
  FROM staging_events e
       LEFT JOIN staging_songs s
              ON e.song = s.title
                 AND e.artist_name = s.artist_name
 WHERE e.page = 'NextSong'
 ORDER BY e.ts;
""")

user_table_insert = ("""
INSERT INTO users(
SELECT user_id
     , first_name
     , last_name
     , gender
     , level
  FROM (SELECT ROW_NUMBER() OVER(
               PARTITION BY user_id ORDER BY ts DESC) AS row_num 
             , user_id
             , first_name
             , last_name
             , gender
             , level
             , ts
          FROM staging_events
         WHERE page = 'NextSong')
 WHERE row_num = 1
);
""")

song_table_insert = ("""
INSERT INTO songs (
SELECT DISTINCT song_id
              , title
              , artist_id
              , year
              , duration
           FROM staging_songs
);
""")

artist_table_insert = ("""
INSERT INTO artists (
SELECT DISTINCT artist_id
              , artist_name
              , artist_location
              , artist_latitude
              , artist_longitude
           FROM staging_songs
);
""")

time_table_insert = ("""
INSERT INTO time (
SELECT DISTINCT ts
              , EXTRACT(hour FROM ts) AS hour
              , EXTRACT(day FROM ts) AS day
              , EXTRACT(week FROM ts) AS week
              , EXTRACT(month FROM ts) AS month
              , EXTRACT(year FROM ts) AS year
              , EXTRACT(weekday FROM ts) AS weekday
           FROM staging_events
          WHERE page = 'NextSong'
);
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
