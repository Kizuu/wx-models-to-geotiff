CREATE TABLE wxdata.log
(
    model text COLLATE pg_catalog."default",
    level text COLLATE pg_catalog."default",
    "timestamp" timestamp with time zone,
    message text COLLATE pg_catalog."default",
    agent text COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE wxdata.log TO eolus;
