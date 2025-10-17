
CREATE TABLE miningnow_algorithms (
    algorithm_name VARCHAR(100) NOT NULL, 
    algorithm_slug VARCHAR(100) NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (algorithm_name)
)

