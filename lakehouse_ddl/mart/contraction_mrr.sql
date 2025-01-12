CREATE OR REPLACE VIEW subscription_mart.contraction_mrr as
    SELECT  *, 
        diff_price as mrr
    from subscription_mart.enriched_subscription_monthly_status
    where renew_status = TRUE 
        and diff_price < 0
;