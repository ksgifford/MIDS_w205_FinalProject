drop table seattle_crime_raw;

create table seattle_crime_raw (
 cad_cdw_id                  varchar(100),
 cad_event_number            bigint,
 general_offense_number      varchar(100),
 event_clearance_code        varchar(100),
 event_clearance_description varchar(500),
 event_clearance_subgroup    varchar(100),
 event_clearance_group          varchar(100),
 event_clearance_date        timestamp,
  hundred_block_location          varchar(100),
 district_sector             varchar(100),
 zonebeat                   varchar(100),
 census_tract                varchar(100),
 longitude                    float,
 latitude                   float,
 incident_location                     varchar(100),
 initial_type_description           varchar(500),
 initial_type_subgroup             varchar(100),
 initial_type_group                    varchar(100),
 at_scene_time                         timestamp);

COPY seattle_crime_raw FROM '/data/MIDS_w205_FinalProject/raw_data/crime/Seattle_Police_Department_911_Incident_Response.csv' CSV HEADER QUOTE '"';

drop table seattle_crime;

CREATE TABLE seattle_crime
        AS
        SELECT
          cad_cdw_id,
          cad_event_number,
          census_tract,
          district_sector,
          event_clearance_code,
          general_offense_number,
          event_clearance_description,
          hundred_block_location,
          incident_location,
          initial_type_description,
          coalesce(initial_type_group, event_clearance_group)            AS initial_type_group,
          longitude,
          latitude,
          zonebeat,
          substring(cast(coalesce(at_scene_time, event_clearance_date)  as text) from 1 for 4)    AS at_scene_year,
          substring(cast(coalesce(at_scene_time, event_clearance_date) as text) from 6 for 2)    AS at_scene_month,
          substring(cast(coalesce(at_scene_time, event_clearance_date) as text) from 9 for 2)    AS at_scene_day,
          substring(cast(coalesce(at_scene_time, event_clearance_date) as text) from 12)      AS at_scene_time,
          coalesce(at_scene_time, event_clearance_date)                  AS at_scene_date_time,
          to_char(coalesce(at_scene_time, event_clearance_date), 'day') AS day_name,
          to_char(coalesce(at_scene_time, event_clearance_date), 'd') AS day_number,
          substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from  1  for 4)    AS event_clearance_year,
          substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from  6  for 2)    AS event_clearance_month,
          substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 9  for 2)    AS event_clearance_day,
          substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from  12)      AS event_clearance_time,
          coalesce(event_clearance_date, at_scene_time)                  AS event_clearance_date_time,
          CASE
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 4 AND 8
             THEN 'EARLY_MORNING'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 9 AND 12
            THEN 'LATE_MORNING'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 13 AND 15
            THEN 'EARLY_AFTERNOON'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 16 AND 18
            THEN 'LATE_AFTERNOON'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 19 AND 21
            THEN 'EVENING'
            ELSE 'NIGHT'
         END AS time_bucket,
         CASE
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 4 AND 8
             THEN '1_EARLY_MORNING'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 9 AND 12
            THEN '2_LATE_MORNING'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 13 AND 15
            THEN '3_EARLY_AFTERNOON'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 16 AND 18
            THEN '4_LATE_AFTERNOON'
            WHEN
              cast(substring(cast(coalesce(event_clearance_date, at_scene_time) as text) from 12 for 2) as int)
              BETWEEN 19 AND 21
            THEN '5_EVENING'
            ELSE '6_NIGHT'
         END AS time_bucket_sort,
         CASE
           WHEN event_clearance_group IN ('BIKE', 'BURGLARY', 'SHOPLIFTING', 'AUTO THEFTS', 'ROBBERY', 'OTHER PROPERTY')
           THEN 'THEFT'
           WHEN event_clearance_group IN ('ARREST', 'TRESPASS', 'RECKLESS BURNING', 'NUISANCE, MISCHIEF', 'PROPERTY DAMAGE', 'DISTURBANCES', 'PROPERTY - MISSING, FOUND', 'PUBLIC GATHERINGS', 'HAZARDS', 'MISCELLANEOUS MISDEMEANORS', 'ANIMAL COMPLAINTS','NUISANCE, MISCHIEF')
           THEN 'LIGHT CRIME'
           WHEN event_clearance_group IN ('ASSAULTS', 'HOMICIDE', 'DRIVE BY (NO INJURY)')
           THEN 'HEAVY CRIME'
           WHEN event_clearance_group IN ('FALSE ALARMS', 'FRAUD CALLS', 'FALSE ALACAD')
           THEN 'FALSE ALARMS'
           WHEN event_clearance_group IN ('LEWD CONDUCT', 'LIQUOR VIOLATIONS', 'OTHER VICE', 'VICE CALLS', 'PROSTITUTION','FAILURE TO REGISTER (SEX OFFENDER)','NARCOTICS COMPLAINTS','MOTOR VEHICLE COLLISION INVESTIGATION')
           THEN 'VICE'
           WHEN event_clearance_group IN ('TRAFFIC RELATED CALLS', 'ACCIDENT INVESTIGATION', 'HARBOR CALLS')
           THEN 'VEHICLE'
           WHEN event_clearance_group IN ('PERSON DOWN/INJURY', 'WEAPONS CALLS', 'BEHAVIORAL HEALTH', 'THREATS, HARASSMENT', 'PERSONS - LOST, FOUND, MISSING','PROWLER', 'CAR PROWL', 'MENTAL HEALTH', 'SUSPICIOUS CIRCUMSTANCES')
           THEN 'POSSIBLE CRIME'
           ELSE 'OTHER'
          END AS crime_type,
          CASE
            WHEN coalesce(event_clearance_group, initial_type_group) IN
                          ('THREATS, HARASSMENT', 'ROBBERY', 'ASSAULTS', 'DRIVE BY (NO INJURY)')
                 THEN TRUE
           ELSE FALSE
           END AS is_violent,
          CASE
              WHEN (coalesce(event_clearance_group, initial_type_group)
             IN ('FRAUD CALLS', 'FALSE ALARMS'))
                         THEN TRUE
               ELSE
                          FALSE
              END AS is_false_alarm,
          coalesce(event_clearance_group, initial_type_group)             AS event_clearance_group,
          event_clearance_subgroup                                   AS event_clearance_subgroup
        FROM seattle_crime_raw
WHERE coalesce(event_clearance_group, initial_type_group) NOT IN ('FRAUD CALLS', 'FALSE ALARMS', 'FALSE ALACAD')
AND cad_cdw_id ~ '^[a-z0-9]*$'
        ;
