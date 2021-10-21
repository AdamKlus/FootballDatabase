import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
events_table_drop = "DROP TABLE IF EXISTS events;"
homeaway_table_drop = "DROP TABLE IF EXISTS homeaway;"
teams_table_drop = "DROP TABLE IF EXISTS teams;"
competitions_table_drop = "DROP TABLE IF EXISTS competitions;"
times_table_drop = "DROP TABLE IF EXISTS times;"

# TRUNCATE TABLES
staging_events_truncate = "TRUNCATE TABLE staging_events;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
	country text,
	competition text,
	time text,
	home text,
	away text,
    date text
);
""")

events_table_create= ("""
CREATE TABLE events (
	event_id int IDENTITY(0,1),
	start_time timestamp NOT NULL,
	home_id int NOT NULL,
	away_id int NOT NULL,
	competition_id int NOT NULL,
    PRIMARY KEY(event_id),
    FOREIGN KEY(start_time) references times(start_time),
	FOREIGN KEY(home_id) references teams(team_id),
	FOREIGN KEY(away_id) references teams(team_id),
    FOREIGN KEY(competition_id) references competitions(competition_id)
);
""")

teams_table_create= ("""
CREATE TABLE teams (
    team_id int IDENTITY(0,1),
	name text NOT NULL,
    country text NOT NULL,
	alternative_names text,
    PRIMARY KEY(team_id)
);
""")

competitions_table_create= ("""
CREATE TABLE competitions (
    competition_id int IDENTITY(0,1),
	name text NOT NULL,
    country text NOT NULL,
    alternative_names text,
    PRIMARY KEY(competition_id)
);
""")

times_table_create = ("""
CREATE TABLE times (
	start_time timestamp NOT NULL,
	hour int,
	day int,
	week int,
	month text,
	year int,
	weekday text,
	PRIMARY KEY(start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}' 
credentials 'aws_iam_role={}'
csv 
ignoreheader 1;
""").format(config.get("S3", "EVENTS_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

competition_table_insert = ("""
INSERT INTO competitions(name,country)
    SELECT DISTINCT competition, country
    FROM staging_events
""")

teams_table_insert = ("""
INSERT INTO teams(name,country)
    SELECT DISTINCT name, country 
    FROM (
        SELECT home AS name, country 
        FROM staging_events
        UNION
        SELECT away AS name, country 
        FROM staging_events
        )
""")

events_table_insert = ("""
INSERT INTO events(start_time,home_id,away_id,competition_id)
	SELECT TO_TIMESTAMP(CONCAT(e.date,e.time),'YYYYMMDDHH:MI'), home.team_id, away.team_id, c.competition_id
	FROM staging_events e
    JOIN competitions c
    ON e.competition = c.name AND e.country = c.country
    JOIN teams home
    ON e.home = home.name and e.country = home.country
    JOIN teams away
    ON e.away = away.name and e.country = away.country
""")

times_table_insert = ("""
INSERT INTO times(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
        extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
    FROM events
""")

competition_table_update = ("""
INSERT INTO competitions(name,country)
    SELECT u.competition, u.country
    FROM (
    	SELECT DISTINCT competition, country
    	FROM staging_events    	
    ) u
    LEFT JOIN competitions c
    ON 
    u.competition = c.name
    AND
    u.country = c.country
    WHERE 
    c.name IS NULL
""")

teams_table_update = ("""
INSERT INTO teams(name,country)
    SELECT u.name, u.country
    FROM (
    	SELECT DISTINCT name, country 
    	FROM (
        	SELECT home AS name, country 
        	FROM staging_events
        	UNION
        	SELECT away AS name, country 
        	FROM staging_events
        	)  	
    	) u
    LEFT JOIN teams t
    ON 
    u.name = t.name
    AND
    u.country = t.country
    WHERE 
    t.name IS NULL
""")

# QUALITY CHECKS

missing_dates = ("""
SELECT DISTINCT start_time::timestamp::date as date 
FROM events 
ORDER BY date
""")

events_count = ("""
SELECT COUNT(*) FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, teams_table_create, competitions_table_create, times_table_create, events_table_create]
drop_table_queries = [staging_events_table_drop, events_table_drop, homeaway_table_drop, teams_table_drop, competitions_table_drop, times_table_drop]
copy_table_queries = [staging_events_copy]
insert_table_queries = [competition_table_insert, teams_table_insert, events_table_insert, times_table_insert]
truncate_table_queries = [staging_events_truncate]
update_table_queries = [competition_table_update, teams_table_update, events_table_insert, times_table_insert]