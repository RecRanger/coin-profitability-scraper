
CREATE TABLE cryptodelver_coins (
    coin_slug VARCHAR(200) NOT NULL, 
    coin_name VARCHAR(200) NOT NULL, 
    algo_name VARCHAR(200), 
    algo_slug VARCHAR(200), 
    reported_proof_type VARCHAR(200), 
    reported_market_cap VARCHAR(200), 
    reported_price_usd VARCHAR(200), 
    reported_volume VARCHAR(200), 
    reported_pct_change_24h FLOAT, 
    reported_pct_change_7d FLOAT, 
    volume_usd BIGINT, 
    market_cap_usd BIGINT, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (coin_slug)
)

