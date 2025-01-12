CREATE OR REPLACE VIEW subscription_mart.enriched_subscription_monthly_status as 
    SELECT  *, NVL(LAG(net_price) OVER (PARTITION BY subscription_id ORDER BY year_month), 0) as prev_net_price,
    net_price - prev_net_price as diff_price
    FROM subscription.subscription_monthly_status_fct;