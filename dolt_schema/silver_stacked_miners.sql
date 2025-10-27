
CREATE TABLE silver_stacked_miners (
    source_site VARCHAR(100) NOT NULL, 
    miner_name VARCHAR(200) NOT NULL, 
    algo_name VARCHAR(100) NOT NULL, 
    reported_algo_name VARCHAR(100) NOT NULL, 
    miner_type VARCHAR(4) NOT NULL, 
    source_table VARCHAR(100) NOT NULL, 
    hashrate_hashes_per_second BIGINT NOT NULL, 
    cooling_type VARCHAR(200), 
    price_usd FLOAT, 
    power_watts FLOAT, 
    weight_kg FLOAT, 
    announcement_date DATE, 
    launch_date DATE, 
    miner_created_at DATETIME NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (source_site, miner_name, algo_name)
)

