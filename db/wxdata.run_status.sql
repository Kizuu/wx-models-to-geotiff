CREATE TABLE wxdata.run_status
(
    model text COLLATE pg_catalog."default",
    "timestamp" timestamp with time zone,
    status text COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE wxdata.run_status TO eolus;