
CREATE TABLE silver_stacked_coins (
    source_site VARCHAR(100) NOT NULL, 
    coin_unique_source_id VARCHAR(100) NOT NULL, 
    coin_name VARCHAR(100) NOT NULL, 
    algo_name VARCHAR(100), 
    coin_url VARCHAR(500), 
    source_table VARCHAR(100) NOT NULL, 
    coin_symbol VARCHAR(100), 
    market_cap_usd BIGINT, 
    volume_24h_usd BIGINT, 
    founded_date DATE, 
    coin_created_at DATETIME NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (source_site, coin_unique_source_id)
)

