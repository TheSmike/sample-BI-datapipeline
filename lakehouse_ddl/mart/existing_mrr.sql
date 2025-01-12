CREATE OR REPLACE VIEW subscription_mart.existing_mrr as 
    SELECT  *, 
        net_price as mrr
    from subscription_mart.enriched_subscription_monthly_status
    where renew_status = TRUE 
        and diff_price = 0 AND net_price > 0
;