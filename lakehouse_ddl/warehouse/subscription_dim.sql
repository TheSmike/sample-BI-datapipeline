CREATE TABLE subscription.subscription_dim (
    subscription_id     STRING      NOT NULL    COMMENT 'Id of the subscription',
    category            STRING      NOT NULL    COMMENT 'Category of the subscription e.g., FREE, PREMIUM or ENTERPRISE',
    category_variant    STRING                  COMMENT 'Define possibly variant on the subscription category',
    creation_ts         TIMESTAMP   NOT NULL    COMMENT 'Define possibly variant on the subscription category',
    valid_from          TIMESTAMP   NOT NULL    COMMENT 'Lower validity limit (SCD Type 2 table)',
    valid_to            TIMESTAMP   NOT NULL    COMMENT 'Upper validity limit (SCD Type 2 table)'
)
LOCATION 's3://sample-bi-datapipeline/subscription/subscription_dim/'
COMMENT 'Dimension table of subscriptions';