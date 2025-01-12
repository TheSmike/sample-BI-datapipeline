CREATE TABLE subscription.subscription_monthly_status_fct (
    id                  STRING      NOT NULL COMMENT 'PK of the table',
    year_month          STRING      NOT NULL COMMENT 'The reference month  in the form YYYY-MM eg., 2024-12',
    net_price           DECIMAL     NOT NULL COMMENT 'Net price of the subscription in the reference month',
    net_revenue         DECIMAL     NOT NULL COMMENT 'Net revenue of the subscription in the reference month, it could differ from net_price due to temporary discount or addons',
    renew_status        BOOLEAN     NOT NULL COMMENT 'Indicates if the subscription is still active',
    suspend_status      BOOLEAN     NOT NULL COMMENT 'Indicates if the subscription has been temporarily suspended',
    -- other fields
    subscription_id     STRING      NOT NULL COMMENT 'Id of the subscription',
    customer_id         STRING      NOT NULL COMMENT 'Id of the customer'
)
LOCATION 's3://sample-bi-datapipeline/subscription/subscription_monthly_status_fct/'
COMMENT 'Fact table of the monthly status of the subscription, it represents the status of the subscription in the month';