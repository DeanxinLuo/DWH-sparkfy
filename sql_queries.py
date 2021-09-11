import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
s3_role = config.get('IAM_ROLE','ARN')

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
    create table if not exists staging_events(
        artist Varchar,
        auth VARCHAR(12),
        firstName VARCHAR,
        gender            VARCHAR,
        itemInSession     INT,
        lastName          VARCHAR,
        length            FLOAT8,
        level             VARCHAR,
        location          VARCHAR,
        method            VARCHAR,
        page              VARCHAR,
        registration      VARCHAR,
        sessionId         INT,
        song              VARCHAR,
        status            INT,
        ts                BIGINT,
        userAgent         VARCHAR,
        userId            INT  
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS "staging_songs" (
        artist_id VARCHAR,
        artist_latitude DECIMAL,
        artist_location VARCHAR,
        artist_longitude DECIMAL,
        artist_name VARCHAR,
        duration DECIMAL,
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR,
        year INTEGER
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
        level            VARCHAR NOT NULL DISTKEY
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id         VARCHAR SORTKEY PRIMARY KEY,
        title           VARCHAR NOT NULL,
        artist_id       VARCHAR NOT NULL DISTKEY,
        year            INT,
        duration        NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id     VARCHAR SORTKEY PRIMARY KEY,
        name          VARCHAR NOT NULL,
        location      VARCHAR,
        latitude      FLOAT8,
        longitude     FLOAT8
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
    CREDENTIALS 'aws_iam_role={s3_role}'
    JSON {} 
    COMPUPDATE OFF REGION 'us-west-2';
""")

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={s3_role}'
    JSON 'auto'
    COMPUPDATE OFF REGION 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time,
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
        LEFT JOIN staging_songs ss
            ON se.song = ss.title
            AND se.artist = ss.artist_name
            AND se.length = ss.duration
    WHERE se.userId IS NOT NULL
        AND ss.artist_id IS NOT NULL
        AND se.level IS NOT NULL
        AND se.sessionId IS NOT NULL
        AND ss.song_id IS NOT NULL
        AND se.ts IS NOT NULL
        AND se.sessionId NOT IN (SELECT DISTINCT session_id FROM songplays)
        AND se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    (
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
    FROM staging_events
    WHERE userId IS NOT NULL
        AND level IS NOT NULL
        AND userId NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
    INSERT INTO songs
    (
      song_id,
      title,
      artist_id,
      year,
      duration
    )
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
        AND title IS NOT NULL
        AND artist_id IS NOT NULL
        AND song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
    INSERT INTO artists
    (
      artist_id,
      name,
      location,
      latitude,
      longitude
    )
    SELECT
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
        AND artist_name is NOT NULL
        AND artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
    
""")

time_table_insert = ("""
    INSERT INTO time
    (
      start_time,
      hour,
      day,
      week,
      month,
      year,
      weekday
    )
    SELECT
        DISTINCT start_time,
        EXTRACT(HOUR FROM start_time),
        EXTRACT(DAY FROM start_time),
        EXTRACT(WEEK FROM start_time),
        EXTRACT(MONTH FROM start_time),
        EXTRACT(YEAR FROM start_time),
        EXTRACT(WEEKDAY FROM start_time)
    FROM (SELECT
            TIMESTAMP 'epoch' + ts/1000*INTERVAL '1 second' AS start_time
        FROM staging_events
            WHERE ts IS NOT NULL) sub
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,time_table_create,song_table_create,artist_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
