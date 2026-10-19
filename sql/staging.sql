-- Staging layer: cast types and standardise values.
-- Nothing is dropped here; invalid values become NULL and are caught by the
-- data-quality rules, so every raw row stays traceable (row_id = raw rowid).

DROP TABLE IF EXISTS stg_prices;

CREATE TABLE stg_prices AS
WITH cleaned AS (
    SELECT
        rowid                                 AS row_id,
        TRIM(assessment_date)                 AS date_txt,
        UPPER(NULLIF(TRIM(commodity), ''))    AS commodity,
        UPPER(NULLIF(TRIM(hub), ''))          AS hub,
        NULLIF(TRIM(price), '')               AS price_txt,
        UPPER(NULLIF(TRIM(currency), ''))     AS currency,
        LOWER(NULLIF(TRIM(unit), ''))         AS unit_lc,
        NULLIF(TRIM(source), '')              AS source,
        _batch_id,
        _source_file
    FROM raw_prices
)
SELECT
    row_id,
    -- strict ISO date: date() must round-trip to the same text
    CASE WHEN date(date_txt) = date_txt THEN date_txt END          AS assessment_date,
    commodity,
    hub,
    -- numeric check before CAST (SQLite would silently turn 'n/a' into 0.0)
    CASE
        WHEN price_txt IS NULL
          OR price_txt GLOB '*[^0-9.eE+-]*'
          OR price_txt NOT GLOB '*[0-9]*'                         THEN NULL
        WHEN unit_lc = 'kwh' THEN CAST(price_txt AS REAL) * 1000.0  -- EUR/kWh -> EUR/MWh
        ELSE CAST(price_txt AS REAL)
    END                                                            AS price,
    price_txt                                                      AS price_raw,
    currency,
    CASE WHEN unit_lc IN ('mwh', 'kwh') THEN 'MWh' ELSE unit_lc END AS unit,
    source,
    _batch_id,
    _source_file
FROM cleaned;
