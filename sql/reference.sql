-- Reference data: which gas hub feeds which power market (used for spark spreads).
DROP TABLE IF EXISTS ref_hub_mapping;
CREATE TABLE ref_hub_mapping (
    power_hub TEXT PRIMARY KEY,
    gas_hub   TEXT NOT NULL
);
INSERT INTO ref_hub_mapping (power_hub, gas_hub) VALUES
    ('DE', 'THE'),   -- Germany power  <- Trading Hub Europe gas
    ('NL', 'TTF');   -- Netherlands power <- Title Transfer Facility gas
