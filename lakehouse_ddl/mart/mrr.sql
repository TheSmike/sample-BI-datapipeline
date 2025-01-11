CREATE OR REPLACE VIEW subscription_mart.mrr as 
    SELECT *, 
        net_price as mrr
    from subscription_mart.enriched_subscription_monthly_status
    where renew_status = TRUE;