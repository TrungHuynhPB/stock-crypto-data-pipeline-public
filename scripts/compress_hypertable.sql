SELECT
    *
FROM
    timescaledb_information.hypertables
WHERE
    hypertable_name = 'raw_corporates'
    
    
 SELECT add_compression_policy(
    'raw_corporates',
    -- Compress all chunks that contain data older than 7 days
    compress_after => INTERVAL '7 days'
);

ALTER TABLE public.raw_corporates SET (
    timescaledb.compress = TRUE,
    timescaledb.compress_segmentby = 'company_id',
    timescaledb.compress_orderby = 'load_timestamp'
);