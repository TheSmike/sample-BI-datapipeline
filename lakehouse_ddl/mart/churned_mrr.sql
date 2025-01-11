CREATE OR REPLACE VIEW subscription_mart.churned_mrr as 
    SELECT *, 
        net_price * -1 as mrr
    FROM subscription_mart.enriched_subscription_monthly_status
    WHERE renew_status = FALSE;