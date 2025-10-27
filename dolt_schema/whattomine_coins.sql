
CREATE TABLE whattomine_coins (
    whattomine_id BIGINT NOT NULL, 
    coin_name VARCHAR(100) NOT NULL, 
    algorithm VARCHAR(100) NOT NULL, 
    algo_param_name VARCHAR(10), 
    tag VARCHAR(100) NOT NULL, 
    is_lagging BOOLEAN NOT NULL, 
    is_testing BOOLEAN NOT NULL, 
    last_update DATETIME NOT NULL, 
    network_hashrate FLOAT NOT NULL, 
    last_block BIGINT NOT NULL, 
    block_time VARCHAR(100) NOT NULL, 
    market_cap_usd FLOAT NOT NULL, 
    block_reward FLOAT NOT NULL, 
    difficulty FLOAT NOT NULL, 
    exchanges_json VARCHAR(10000) NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (whattomine_id)
)

