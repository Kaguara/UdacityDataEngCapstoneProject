staging_events_table_drop_sql = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop_sql = "DROP TABLE IF EXISTS staging_songs"
CREATE_STAGING_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_events (
staging_events_key INTEGER IDENTITY(0,1),
artist             VARCHAR(MAX),            
auth               VARCHAR(MAX),
firstName          VARCHAR(MAX),
gender             VARCHAR(MAX),
itemInSession      INTEGER,
lastName           VARCHAR(MAX),
length             DOUBLE PRECISION,
level              VARCHAR(MAX),
location           VARCHAR(MAX),
method             VARCHAR(MAX),
page               VARCHAR(MAX),
registration       DECIMAL(15,1),
sessionId          INTEGER,      
song               VARCHAR(MAX),
status             INTEGER,
ts                 TIMESTAMP,
userAgent          VARCHAR(MAX),
userId             INTEGER distkey
);
"""

CREATE_STAGING_SONGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_songs (
staging_songs_key INTEGER IDENTITY(0,1),
song_id           VARCHAR(MAX),            
artist_id         VARCHAR(MAX) distkey,
num_songs         INTEGER,
artist_latitude   REAL,
artist_longitude  REAL,
artist_location   VARCHAR(MAX),
artist_name       VARCHAR(MAX),
title             VARCHAR(MAX),      
duration          REAL,
year              INTEGER
);
"""