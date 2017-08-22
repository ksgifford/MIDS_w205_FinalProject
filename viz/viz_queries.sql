-- Query bus stops by crime type.
SELECT
    bus_route_stop_seattle.stop_name,
    bus_route_stop_seattle.stop_lat,
    bus_route_stop_seattle.stop_lon,
    seattle_crime.crime_type,
    count(spjoin.cad_cdw_id) as crime_count,
    (
        bus_route_stop_seattle.stop_name||' - Incidents: '||COALESCE(count(spjoin.cad_cdw_id),0)
    ) as crime_string
FROM bus_route_stop_seattle
INNER JOIN spjoin
ON bus_route_stop_seattle.stop_id = spjoin.stop_id
INNER JOIN seattle_crime
ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
WHERE seattle_crime.crime_type = 'HEAVY CRIME'
GROUP BY
    bus_route_stop_seattle.stop_name,
    seattle_crime.crime_type,
    bus_route_stop_seattle.stop_lat,
    bus_route_stop_seattle.stop_lon
ORDER BY count(spjoin.cad_cdw_id) DESC
LIMIT 100;




-- Query all stops and crime counts by type for a given route.
SELECT
    bus_route_stop_seattle.stop_name,
    bus_route_stop_seattle.stop_lat,
    bus_route_stop_seattle.stop_lon,
    seattle_crime.crime_type,
    count(spjoin.cad_cdw_id)
FROM bus_route_stop_seattle
INNER JOIN spjoin
ON bus_route_stop_seattle.stop_id = spjoin.stop_id
INNER JOIN seattle_crime
ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
WHERE bus_route_stop_seattle.route_short_name = '"49"'
GROUP BY
    bus_route_stop_seattle.stop_name,
    seattle_crime.crime_type,
    bus_route_stop_seattle.stop_lat,
    bus_route_stop_seattle.stop_lon
ORDER BY bus_route_stop_seattle.stop_name DESC;




-- Query all stops and crime counts by type for the 10 highest-crime routes.
SELECT
    Base.stop_id,
    Base.route_short_name,
    Base.route_crime_rank,
    Base.stop_name,
    Base.stop_lat,
    Base.stop_lon,
    total.total_crime_count,
    hc.heavy_crime_count,
    lc.light_crime_count,
    vc.vice_crime_count,
    tc.theft_crime_count,
    (
        'Route '||TRIM(both '"' from Base.route_short_name)||' - '||
        TRIM(both '"' from Base.stop_name)||' ('||
        'All Crime: '||COALESCE(total.total_crime_count,0)||
        ' Heavy Crime: '||COALESCE(hc.heavy_crime_count,0)||
        ' Light Crime: '||COALESCE(lc.light_crime_count,0)||
        ' Vice: '||COALESCE(vc.vice_crime_count,0)||
        ' Theft: '||COALESCE(tc.theft_crime_count,0)||')'
    ) as crime_string
FROM (
    SELECT
        bus_route_stop_seattle.stop_id,
        bus_route_stop_seattle.route_short_name,
        crime_rank.total_crime_rank_count as route_crime_rank,
        bus_route_stop_seattle.stop_name,
        bus_route_stop_seattle.stop_lat,
        bus_route_stop_seattle.stop_lon
    FROM bus_route_stop_seattle
    INNER JOIN spjoin
    ON bus_route_stop_seattle.stop_id = spjoin.stop_id
    INNER JOIN seattle_crime
    ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
    INNER JOIN (
        SELECT
            bus_route_stop_seattle.route_short_name as bus_route,
            RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as total_crime_rank_count
        FROM
            bus_route_stop_seattle
        LEFT JOIN
            spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
        LEFT JOIN
            seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
        GROUP BY
            bus_route
        LIMIT 10
    ) as crime_rank
    ON crime_rank.bus_route = bus_route_stop_seattle.route_short_name
    GROUP BY
        route_crime_rank,
        bus_route_stop_seattle.stop_id,
        bus_route_stop_seattle.route_short_name,
        bus_route_stop_seattle.stop_name,
        bus_route_stop_seattle.stop_lat,
        bus_route_stop_seattle.stop_lon
) as Base
LEFT JOIN (
    SELECT
        bus_route_stop_seattle.stop_id as stop_id,
        count(seattle_crime.cad_cdw_id) as total_crime_count
    FROM seattle_crime
    INNER JOIN spjoin
    ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
    INNER JOIN bus_route_stop_seattle
    ON spjoin.stop_id = bus_route_stop_seattle.stop_id
    GROUP BY bus_route_stop_seattle.stop_id
) as total
ON total.stop_id = Base.stop_id
LEFT JOIN (
    SELECT
        bus_route_stop_seattle.stop_id as stop_id,
        count(seattle_crime.cad_cdw_id) as heavy_crime_count
    FROM seattle_crime
    INNER JOIN spjoin
    ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
    INNER JOIN bus_route_stop_seattle
    ON spjoin.stop_id = bus_route_stop_seattle.stop_id
    WHERE seattle_crime.crime_type = 'HEAVY CRIME'
    GROUP BY bus_route_stop_seattle.stop_id
) as hc
ON hc.stop_id = Base.stop_id
LEFT JOIN (
    SELECT
        bus_route_stop_seattle.stop_id as stop_id,
        count(seattle_crime.cad_cdw_id) as light_crime_count
    FROM seattle_crime
    INNER JOIN spjoin
    ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
    INNER JOIN bus_route_stop_seattle
    ON spjoin.stop_id = bus_route_stop_seattle.stop_id
    WHERE seattle_crime.crime_type = 'LIGHT CRIME'
    GROUP BY bus_route_stop_seattle.stop_id
) as lc
ON lc.stop_id = Base.stop_id
LEFT JOIN (
    SELECT
        bus_route_stop_seattle.stop_id as stop_id,
        count(seattle_crime.cad_cdw_id) as vice_crime_count
    FROM seattle_crime
    INNER JOIN spjoin
    ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
    INNER JOIN bus_route_stop_seattle
    ON spjoin.stop_id = bus_route_stop_seattle.stop_id
    WHERE seattle_crime.crime_type = 'VICE'
    GROUP BY bus_route_stop_seattle.stop_id
) as vc
ON vc.stop_id = Base.stop_id
LEFT JOIN (
    SELECT
        bus_route_stop_seattle.stop_id as stop_id,
        count(seattle_crime.cad_cdw_id) as theft_crime_count
    FROM seattle_crime
    INNER JOIN spjoin
    ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
    INNER JOIN bus_route_stop_seattle
    ON spjoin.stop_id = bus_route_stop_seattle.stop_id
    WHERE seattle_crime.crime_type = 'THEFT'
    GROUP BY bus_route_stop_seattle.stop_id
) as tc
ON tc.stop_id = Base.stop_id
ORDER BY Base.route_crime_rank, Base.route_short_name, Base.stop_id;



-- Query crime type count by time of day.
SELECT
    time_bucket,
    count(cad_cdw_id)
FROM seattle_crime
WHERE crime_type = 'HEAVY CRIME'
GROUP BY time_bucket;




-- Query group by crime type count by time of day.
select
    BaseSet.time_bucket,
    BaseSet.count_total,
    FirstSet.count_heavy_crime,
    SecondSet.count_other,
	ThirdSet.count_theft,
	FourthSet.count_light_crime,
	FifthSet.count_vehicle,
	SixthSet.count_vice,
	SeventhSet.count_possible_crime
from
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_total
	FROM
		seattle_crime
	GROUP BY
		time_bucket
) as BaseSet
left join
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_heavy_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'HEAVY CRIME'
	GROUP BY
		time_bucket
) as FirstSet
on BaseSet.time_bucket = FirstSet.time_bucket
left join
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_other
	FROM
		seattle_crime
	WHERE
		crime_type = 'OTHER'
	GROUP BY
		time_bucket
) as SecondSet
on BaseSet.time_bucket = SecondSet.time_bucket
left join
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_theft
	FROM
		seattle_crime
	WHERE
		crime_type = 'THEFT'
	GROUP BY
		time_bucket
) as ThirdSet
on BaseSet.time_bucket = ThirdSet.time_bucket
left join
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_light_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'LIGHT CRIME'
	GROUP BY
		time_bucket
) as FourthSet
on BaseSet.time_bucket = FourthSet.time_bucket
left join
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_vehicle
	FROM
		seattle_crime
	WHERE
		crime_type = 'VEHICLE'
	GROUP BY
		time_bucket
) as FifthSet
on BaseSet.time_bucket = FifthSet.time_bucket
left join
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_vice
	FROM
		seattle_crime
	WHERE
		crime_type = 'VICE'
	GROUP BY
		time_bucket
) as SixthSet
on BaseSet.time_bucket = SixthSet.time_bucket
left join
(
	SELECT
	    time_bucket,
		count(cad_cdw_id) as count_possible_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'POSSIBLE CRIME'
	GROUP BY
		time_bucket
) as SeventhSet
on BaseSet.time_bucket = SeventhSet.time_bucket
ORDER BY
	CASE FirstSet.time_bucket
	WHEN 'EARLY_MORNING' THEN 1
	WHEN 'LATE_MORNING'  THEN 2
	WHEN 'EARLY_AFTERNOON' THEN 3
	WHEN 'LATE_AFTERNOON' THEN 4
	WHEN 'EVENING' THEN 5
	WHEN 'NIGHT' THEN 6
	END, FirstSet.time_bucket ASC;





-- Query group by crime type count by bus routes.
select
    FirstSet.bus_route,
    FirstSet.total_crime_rank_count,
    FirstSet.total_crime_count,
	SecondSet.hc_crime_rank_count,
	SecondSet.hc_crime_count,
	ThirdSet.o_crime_rank_count,
	ThirdSet.o_crime_count,
	FourthSet.t_crime_rank_count,
	FourthSet.t_crime_count,
	FifthSet.lc_crime_rank_count,
	FifthSet.lc_crime_count,
	SixthSet.v_crime_rank_count,
	SixthSet.v_crime_count,
	SeventhSet.vice_crime_rank_count,
	SeventhSet.vice_crime_count,
	EighthSet.pc_crime_rank_count,
	EighthSet.pc_crime_count
from
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as total_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as total_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	GROUP BY
		bus_route
) as FirstSet
left join
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as hc_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as hc_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'HEAVY CRIME'
	GROUP BY
		bus_route
) as SecondSet
on FirstSet.bus_route = SecondSet.bus_route
left join
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as o_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as o_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'OTHER'
	GROUP BY
		bus_route
) as ThirdSet
on FirstSet.bus_route = ThirdSet.bus_route
left join
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as t_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as t_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'THEFT'
	GROUP BY
		bus_route
) as FourthSet
on FirstSet.bus_route = FourthSet.bus_route
left join
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as lc_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as lc_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'LIGHT CRIME'
	GROUP BY
		bus_route
) as FifthSet
on FirstSet.bus_route = FifthSet.bus_route
left join
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as v_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as v_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'VEHICLE'
	GROUP BY
		bus_route
) as SixthSet
on FirstSet.bus_route = SixthSet.bus_route
left join
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as vice_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as vice_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'VICE'
	GROUP BY
		bus_route
) as SeventhSet
on FirstSet.bus_route = SeventhSet.bus_route
left join
(
	SELECT
		bus_route_stop_seattle.route_short_name as bus_route,
		RANK() OVER(ORDER BY count(seattle_crime.cad_cdw_id) DESC) as pc_crime_rank_count,
		count(seattle_crime.cad_cdw_id) as pc_crime_count
	FROM
		bus_route_stop_seattle
	LEFT JOIN
		spjoin ON bus_route_stop_seattle.stop_id = spjoin.stop_id
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'POSSIBLE CRIME'
	GROUP BY
		bus_route
) as EighthSet
on FirstSet.bus_route = EighthSet.bus_route
ORDER BY
	FirstSet.total_crime_rank_count
LIMIT 10;





-- Query group by crime type count by bus stops.
select
    FirstSet.bus_stop,
    FirstSet.stop_name,
    FirstSet.total_crime_rank_count,
    FirstSet.total_crime_count,
	SecondSet.hc_crime_rank_count,
	SecondSet.hc_crime_count,
	ThirdSet.o_crime_rank_count,
	ThirdSet.o_crime_count,
	FourthSet.t_crime_rank_count,
	FourthSet.t_crime_count,
	FifthSet.lc_crime_rank_count,
	FifthSet.lc_crime_count,
	SixthSet.v_crime_rank_count,
	SixthSet.v_crime_count,
	SeventhSet.vice_crime_rank_count,
	SeventhSet.vice_crime_count,
	EighthSet.pc_crime_rank_count,
	EighthSet.pc_crime_count
from
(
	SELECT
		spjoin.stop_id as bus_stop,
		spjoin.stop_name as stop_name,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as total_crime_rank_count,
		count(spjoin.cad_cdw_id) as total_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	GROUP BY
		bus_stop, stop_name
) as FirstSet
left join
(
	SELECT
		spjoin.stop_id as bus_stop,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as hc_crime_rank_count,
		count(spjoin.cad_cdw_id) as hc_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'HEAVY CRIME'
	GROUP BY
		bus_stop
) as SecondSet
on FirstSet.bus_stop = SecondSet.bus_stop
left join
(
	SELECT
		spjoin.stop_id as bus_stop,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as o_crime_rank_count,
		count(spjoin.cad_cdw_id) as o_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'OTHER'
	GROUP BY
		bus_stop
) as ThirdSet
on FirstSet.bus_stop = ThirdSet.bus_stop
left join
(
	SELECT
		spjoin.stop_id as bus_stop,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as t_crime_rank_count,
		count(spjoin.cad_cdw_id) as t_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'THEFT'
	GROUP BY
		bus_stop
) as FourthSet
on FirstSet.bus_stop = FourthSet.bus_stop
left join
(
	SELECT
		spjoin.stop_id as bus_stop,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as lc_crime_rank_count,
		count(spjoin.cad_cdw_id) as lc_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'LIGHT CRIME'
	GROUP BY
		bus_stop
) as FifthSet
on FirstSet.bus_stop = FifthSet.bus_stop
left join
(
	SELECT
		spjoin.stop_id as bus_stop,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as v_crime_rank_count,
		count(spjoin.cad_cdw_id) as v_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'VEHICLE'
	GROUP BY
		bus_stop
) as SixthSet
on FirstSet.bus_stop = SixthSet.bus_stop
left join
(
	SELECT
		spjoin.stop_id as bus_stop,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as vice_crime_rank_count,
		count(spjoin.cad_cdw_id) as vice_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'VICE'
	GROUP BY
		bus_stop
) as SeventhSet
on FirstSet.bus_stop = SeventhSet.bus_stop
left join
(
	SELECT
		spjoin.stop_id as bus_stop,
		RANK() OVER(ORDER BY count(spjoin.cad_cdw_id) DESC) as pc_crime_rank_count,
		count(spjoin.cad_cdw_id) as pc_crime_count
	FROM
		spjoin
	LEFT JOIN
		seattle_crime ON spjoin.cad_cdw_id = seattle_crime.cad_cdw_id
	WHERE
		crime_type = 'POSSIBLE CRIME'
	GROUP BY
		bus_stop
) as EighthSet
on FirstSet.bus_stop = EighthSet.bus_stop
ORDER BY
	FirstSet.total_crime_rank_count
LIMIT 10;



-- Query group by crime type count by month.
select
    BaseSet.at_scene_month,
    BaseSet.count_total,
    FirstSet.count_heavy_crime,
    SecondSet.count_other,
	ThirdSet.count_theft,
	FourthSet.count_light_crime,
	FifthSet.count_vehicle,
	SixthSet.count_vice,
	SeventhSet.count_possible_crime
from
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_total
	FROM
		seattle_crime
	GROUP BY
		at_scene_month
) as BaseSet
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_heavy_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'HEAVY CRIME'
	GROUP BY
		at_scene_month
) as FirstSet
on BaseSet.at_scene_month = FirstSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_other
	FROM
		seattle_crime
	WHERE
		crime_type = 'OTHER'
	GROUP BY
		at_scene_month
) as SecondSet
on BaseSet.at_scene_month = SecondSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_theft
	FROM
		seattle_crime
	WHERE
		crime_type = 'THEFT'
	GROUP BY
		at_scene_month
) as ThirdSet
on BaseSet.at_scene_month = ThirdSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_light_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'LIGHT CRIME'
	GROUP BY
		at_scene_month
) as FourthSet
on BaseSet.at_scene_month = FourthSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_vehicle
	FROM
		seattle_crime
	WHERE
		crime_type = 'VEHICLE'
	GROUP BY
		at_scene_month
) as FifthSet
on BaseSet.at_scene_month = FifthSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_vice
	FROM
		seattle_crime
	WHERE
		crime_type = 'VICE'
	GROUP BY
		at_scene_month
) as SixthSet
on BaseSet.at_scene_month = SixthSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_possible_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'POSSIBLE CRIME'
	GROUP BY
		at_scene_month
) as SeventhSet
on BaseSet.at_scene_month = SeventhSet.at_scene_month;



-- Query group by crime type count by district.
select
    BaseSet.at_scene_month,
    BaseSet.count_total,
    FirstSet.count_heavy_crime,
    SecondSet.count_other,
	ThirdSet.count_theft,
	FourthSet.count_light_crime,
	FifthSet.count_vehicle,
	SixthSet.count_vice,
	SeventhSet.count_possible_crime
from
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_total
	FROM
		seattle_crime
	GROUP BY
		at_scene_month
) as BaseSet
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_heavy_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'HEAVY CRIME'
	GROUP BY
		at_scene_month
) as FirstSet
on BaseSet.at_scene_month = FirstSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_other
	FROM
		seattle_crime
	WHERE
		crime_type = 'OTHER'
	GROUP BY
		at_scene_month
) as SecondSet
on BaseSet.at_scene_month = SecondSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_theft
	FROM
		seattle_crime
	WHERE
		crime_type = 'THEFT'
	GROUP BY
		at_scene_month
) as ThirdSet
on BaseSet.at_scene_month = ThirdSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_light_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'LIGHT CRIME'
	GROUP BY
		at_scene_month
) as FourthSet
on BaseSet.at_scene_month = FourthSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_vehicle
	FROM
		seattle_crime
	WHERE
		crime_type = 'VEHICLE'
	GROUP BY
		at_scene_month
) as FifthSet
on BaseSet.at_scene_month = FifthSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_vice
	FROM
		seattle_crime
	WHERE
		crime_type = 'VICE'
	GROUP BY
		at_scene_month
) as SixthSet
on BaseSet.at_scene_month = SixthSet.at_scene_month
left join
(
	SELECT
	    at_scene_month,
		count(cad_cdw_id) as count_possible_crime
	FROM
		seattle_crime
	WHERE
		crime_type = 'POSSIBLE CRIME'
	GROUP BY
		at_scene_month
) as SeventhSet
on BaseSet.at_scene_month = SeventhSet.at_scene_month;
