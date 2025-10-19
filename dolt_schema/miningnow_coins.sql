
CREATE TABLE miningnow_coins (
    coin_name VARCHAR(100) NOT NULL, 
    coin_slug VARCHAR(100) NOT NULL, 
    ticker VARCHAR(50) NOT NULL, 
    reported_founded VARCHAR(100), 
    algorithm VARCHAR(100), 
    price_usd FLOAT, 
    market_cap_usd BIGINT, 
    volume_usd BIGINT, 
    change_24h FLOAT, 
    founded_date DATE, 
    chart_svg_url VARCHAR(1000), 
    chart_json_url VARCHAR(1000), 
    icon_light_url VARCHAR(1000), 
    icon_dark_url VARCHAR(1000), 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (coin_name)
)

