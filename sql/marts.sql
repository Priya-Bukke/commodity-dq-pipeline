-- Business-ready data products built only from rows that passed all
-- error-severity data-quality rules.

DROP TABLE IF EXISTS mart_daily_prices;
CREATE TABLE mart_daily_prices AS
SELECT
    assessment_date,
    commodity,
    hub,
    ROUND(price, 4)  AS price_eur_mwh,
    dq_warnings,
    source
FROM clean_prices;

-- Data contract: exactly one assessment per day, commodity and hub.
-- If the uniqueness rule is ever removed, this index makes the build fail loudly.
CREATE UNIQUE INDEX ux_mart_daily ON mart_daily_prices (assessment_date, commodity, hub);


DROP TABLE IF EXISTS mart_weekly_prices;
CREATE TABLE mart_weekly_prices AS
SELECT
    strftime('%Y-W%W', assessment_date) AS week,
    commodity,
    hub,
    COUNT(*)                             AS n_days,
    ROUND(AVG(price), 2)                 AS avg_price_eur_mwh,
    ROUND(MIN(price), 2)                 AS min_price_eur_mwh,
    ROUND(MAX(price), 2)                 AS max_price_eur_mwh
FROM clean_prices
GROUP BY 1, 2, 3;


-- Spark spread (excluding carbon cost): power price minus the gas cost of
-- generating 1 MWh, assuming a 50% efficient gas plant (heat rate 2.0).
DROP TABLE IF EXISTS mart_spark_spread;
CREATE TABLE mart_spark_spread AS
SELECT
    p.assessment_date,
    p.hub                                   AS power_hub,
    g.hub                                   AS gas_hub,
    ROUND(p.price, 2)                       AS power_eur_mwh,
    ROUND(g.price, 2)                       AS gas_eur_mwh,
    ROUND(p.price - g.price / 0.50, 2)      AS spark_spread_eur_mwh
FROM clean_prices p
JOIN ref_hub_mapping m ON m.power_hub = p.hub
JOIN clean_prices g    ON g.hub = m.gas_hub
                      AND g.commodity = 'GAS'
                      AND g.assessment_date = p.assessment_date
WHERE p.commodity = 'POWER';
