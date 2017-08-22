DROP TABLE bus_stop_seattle;

CREATE TABLE bus_stop_seattle (
stop_id BIGINT NOT NULL,
stop_name TEXT NOT NULL,
stop_lat DOUBLE PRECISION NOT NULL,
stop_lon DOUBLE PRECISION NOT NULL
);

DROP TABLE bus_route_stop_seattle;

CREATE TABLE bus_route_stop_seattle (
route_short_name TEXT NOT NULL,
stop_name TEXT NOT NULL,
stop_id BIGINT NOT NULL,
stop_lat DOUBLE PRECISION NOT NULL,
stop_lon DOUBLE PRECISION NOT NULL
);
