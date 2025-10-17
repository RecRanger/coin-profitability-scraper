
CREATE TABLE cryptoslate_coins (
    coin_slug VARCHAR(200) NOT NULL, 
    coin_name VARCHAR(200) NOT NULL, 
    hash_algo VARCHAR(200), 
    market_cap_usd BIGINT, 
    earliest_year_in_description INTEGER, 
    earliest_logo_date DATE, 
    html_file_size_bytes BIGINT NOT NULL, 
    reported_blockchain VARCHAR(200), 
    reported_consensus VARCHAR(200), 
    reported_hash_algorithm VARCHAR(200), 
    reported_org_structure VARCHAR(200), 
    reported_development_status VARCHAR(200), 
    reported_open_source VARCHAR(200), 
    reported_hard_wallet_support VARCHAR(200), 
    reported_block_time VARCHAR(200), 
    reported_staking_apr VARCHAR(200), 
    reported_inflation VARCHAR(200), 
    url VARCHAR(200), 
    earliest_year INTEGER, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (coin_slug)
)

