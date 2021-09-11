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
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR ,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INT,
        last_name VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        session_id NUMERIC,
        song VARCHAR,
        status INT,
        timestamp BIGINT,
        user_agent VARCHAR,
        user_id INT)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS "staging_songs" (
    "artist_id" VARCHAR NOT NULL,
    "artist_latitude" DECIMAL,
    "artist_location" VARCHAR,
    "artist_longitude" DECIMAL,
    "artist_name" VARCHAR,
    "duration" DECIMAL,
    "num_songs" INTEGER,
    "song_id" VARCHAR,
    "title" VARCHAR,
    "year" INTEGER
);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL SORTKEY DISTKEY,
        start_time TIMESTAMP REFERENCES time(start_time),
        user_id VARCHAR(18) REFERENCES users(user_id),
        level VARCHAR(20),
        song_id VARCHAR(18) REFERENCES songs(song_id),
        artist_id VARCHAR(18) REFERENCES artists(artist_id),
        session_id INTEGER,
        location VARCHAR(50),
        user_agent VARCHAR
)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id          INT SORTKEY PRIMARY KEY,
        first_name       VARCHAR,
        last_name        VARCHAR,
        gender           VARCHAR,
        level            VARCHAR
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(18) PRIMARY KEY NOT NULL SORTKEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR(18) NOT NULL REFERENCES artists(artist_id),
    year SMALLINT NOT NULL,
    duration SMALLINT NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY NOT NULL SORTKEY,
    name VARCHAR,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL
);
""")
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time    TIMESTAMP SORTKEY PRIMARY KEY,
        hour          INT NOT NULL,
        day           INT NOT NULL,
        week          INT NOT NULL,
        month         INT NOT NULL,
        year          INT NOT NULL DISTKEY,
        weekday       INT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    json {}
    """).format(config.get('S3', 'LOG_DATA'),
             config.get('IAM_ROLE', 'ARN'),
             config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    json 'auto'
    """).format(config.get('S3', 'SONG_DATA'),
             config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    user_id, 
    level,
    start_time,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
    userId as user_id, 
    level,
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
    song_id,
    ss.artist_id,
    sessionId as session_id,
    location,
    userAgent as user_agent
    
FROM staging_events AS se
JOIN staging_songs AS ss
    ON se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level           
    )
SELECT 
    DISTINCT userId,
    firstName,
    lastName,
    gender,
    level
    FROM staging_events AS es1
        WHERE userId IS NOT null
        AND ts = (SELECT max(ts) 
                  FROM staging_events AS es2 
                  WHERE es1.userId = es2.userId)
ORDER BY userId DESC;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    --location,
    latitude,
    longitude
)
SELECT
    artist_id,
    artist_name,
    --artist_location,
    artist_latitude,
    artist_longitude
    
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT  
    DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(week FROM start_time) AS weekday
    
FROM staging_events
WHERE staging_events.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
