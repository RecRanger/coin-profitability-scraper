
CREATE TABLE notify_log_new_algorithms (
    algo_name VARCHAR(100) NOT NULL, 
    created_at DATETIME DEFAULT now() NOT NULL, 
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (algo_name)
)

