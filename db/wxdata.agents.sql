CREATE TABLE wxdata.agents
(
    pid text COLLATE pg_catalog."default",
    start_time timestamp with time zone
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE wxdata.agents TO eolus;