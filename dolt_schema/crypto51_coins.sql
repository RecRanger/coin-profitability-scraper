
CREATE TABLE crypto51_coins (
    coin_name VARCHAR(100) NOT NULL, 
    coin_symbol VARCHAR(100) NOT NULL, 
    algorithm VARCHAR(100) NOT NULL, 
    reported_market_cap VARCHAR(100), 
    reported_hash_rate VARCHAR(100), 
    reported_1h_attack_cost VARCHAR(100), 
    reported_nicehash_capability_percent VARCHAR(100), 
    url VARCHAR(500) NOT NULL, 
    coin_slug VARCHAR(100) NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (coin_name)
)

