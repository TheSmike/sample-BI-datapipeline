CREATE OR REPLACE VIEW subscription_mart.new_mrr as 
    SELECT  *, 
        net_price as mrr
    from subscription_mart.enriched_subscription_monthly_status
    where renew_status = TRUE 
        and diff_price > 0 AND prev_net_price = 0
;