
CREATE TABLE whattomine_miners (
    miner_algorithm_id VARCHAR(200) NOT NULL, 
    whattomine_miner_id BIGINT NOT NULL, 
    miner_name VARCHAR(200) NOT NULL, 
    release_date DATE, 
    miner_type VARCHAR(4) NOT NULL, 
    algorithm_name VARCHAR(100) NOT NULL, 
    reported_hashrate VARCHAR(100) NOT NULL, 
    power_watts BIGINT, 
    hashrate_hashes_per_second BIGINT NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (miner_algorithm_id)
)

