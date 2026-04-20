CREATE TABLE IF NOT EXISTS occupancy
(
    parking        VARCHAR(512) PRIMARY KEY,
    type           VARCHAR(256),
    label          VARCHAR(256),
    is_version_of  VARCHAR(512),
    modified       TIMESTAMPTZ,
    total_capacity INT,
    current_value  INT,
    operator       VARCHAR(256),
    free_of_charge BOOLEAN,
    url            VARCHAR(512),
    opening_hours  VARCHAR(256),
    latitude       NUMERIC(10,5),
    longitude      NUMERIC(10,5)
);
