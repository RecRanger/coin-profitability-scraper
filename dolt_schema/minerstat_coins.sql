
CREATE TABLE minerstat_coins (
    coin_slug VARCHAR(200) NOT NULL, 
    reported_algorithm VARCHAR(200), 
    reported_difficulty VARCHAR(200), 
    reported_block_reward VARCHAR(200), 
    reported_volume VARCHAR(200), 
    reported_founded VARCHAR(200), 
    reported_network_hashrate VARCHAR(200), 
    reported_revenue VARCHAR(200), 
    reported_block_dag VARCHAR(200), 
    reported_block_epoch VARCHAR(200), 
    volume_usd BIGINT, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (coin_slug)
)

