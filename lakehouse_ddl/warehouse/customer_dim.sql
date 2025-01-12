CREATE TABLE subscription.customer_dim (
    customer_id             STRING      NOT NULL    COMMENT 'Id of the customer',
    registration_ts         TIMESTAMP   NOT NULL    COMMENT 'Registration timestamp',
    registration_country    STRING      NOT NULL    COMMENT 'Registration country'
)
LOCATION 's3://sample-bi-datapipeline/subscription/customer/'
COMMENT 'Dimension table of customers';