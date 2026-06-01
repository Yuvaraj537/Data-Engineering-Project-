CREATE SCHEMA IF NOT EXISTS `project-387ad332-118a-45d4-84f.crypto_data`
OPTIONS (location = 'US');





CREATE OR REPLACE TABLE `project-387ad332-118a-45d4-84f.crypto_data.crypto_records` (
  id                          STRING,
  symbol                      STRING,
  name                        STRING,
  current_price               FLOAT64,
  market_cap                  FLOAT64,
  market_cap_rank             INT64,
  total_volume                FLOAT64,
  high_24h                    FLOAT64,
  low_24h                     FLOAT64,
  price_change_24h            FLOAT64,
  price_change_percentage_24h FLOAT64,
  circulating_supply          FLOAT64,
  total_supply                FLOAT64,
  max_supply                  FLOAT64,
  ath                         FLOAT64,
  atl                         FLOAT64
);




LOAD DATA OVERWRITE `project-387ad332-118a-45d4-84f.crypto_data.crypto_records`
FROM FILES (
  format = 'CSV',
  uris = ['gs://ps-gcp-ingress-bucket-7f3a/raw/provider_retail/crypto_10000_records.csv'],
  skip_leading_rows = 1
);

select * from `project-387ad332-118a-45d4-84f.crypto_data.crypto_records`;

SELECT COUNT(*) AS total_rows
FROM `project-387ad332-118a-45d4-84f.crypto_data.crypto_records`;


SELECT
    id, symbol, name,
    current_price, market_cap, market_cap_rank,
    total_volume, high_24h, low_24h,
    price_change_24h, price_change_percentage_24h,
    circulating_supply, total_supply, max_supply,
    ath, atl
FROM `project-387ad332-118a-45d4-84f.crypto_data.crypto_records`
ORDER BY market_cap_rank ASC
LIMIT 10;








