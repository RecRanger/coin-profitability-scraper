
CREATE TABLE gold_algorithms (
    algo_name VARCHAR(100) NOT NULL, 
    source_sites_json VARCHAR(1000) NOT NULL, 
    source_tables_json VARCHAR(1000) NOT NULL, 
    coin_count BIGINT NOT NULL, 
    earliest_coin_created_at DATE NOT NULL, 
    latest_coin_created_at DATE NOT NULL, 
    earliest_coin VARCHAR(100) NOT NULL, 
    latest_coin VARCHAR(100) NOT NULL, 
    volume_24h_usd FLOAT, 
    market_cap_usd FLOAT, 
    asic_count BIGINT, 
    earliest_asic_announcement_date DATE, 
    earliest_asic_launch_date DATE, 
    earliest_asic_created_at DATE, 
    latest_asic_created_at DATE, 
    reported_aliases_json VARCHAR(1000) NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (algo_name)
)

