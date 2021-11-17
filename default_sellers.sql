-- default sellers
create or replace table seller as
select distinct merchant_token, outstanding_amount_cents/100 as os_dllr
from ledger.pd_mysql_ledger_001__ledger_production.pending_balance_snapshots 
where outstanding_amount_cents < 0
QUALIFY row_number() OVER (PARTITION BY merchant_token ORDER BY updated_at desc) = 1
;

--default date
create or replace table debit_falied as
select distinct user_token, max(returned_at) as default_date
from (
SELECT banksettlemententrytoken                                       AS bank_settlement_entry_token
     , MAX(merchant_token)                                            AS user_token
     , MAX(bse.amount_amount)                                         AS bank_settlement_amount
     , MAX(TO_DATE(to_timestamp(bsr.createdat_instantusec/1000000)))  AS returned_at
     , MAX(reason)                                                    AS reason
FROM ledger.raw_feeds.bank_settlement_return bsr
JOIN ledger.raw_feeds.bank_settlement_entry bse
  ON bse.token = bsr.banksettlemententrytoken
WHERE type = 'DEBIT'
GROUP BY banksettlemententrytoken)
group by 1
;

create or replace table default as
select distinct seller.merchant_token as user_token, default_date, os_dllr
from seller
left join debit_falied debit
on seller.merchant_token = debit.user_token
;

-- all loss
create or replace table loss as 
select 
distinct default.user_token,
sum(loss_cents/100) as loss_dllr
from default 
left join app_risk.app_risk.chargebacks cb
on default.user_token = cb.user_token
and payment_created_at <= default_date
group by 1
;

-- no fa no ato loss
create or replace table no_fa_no_ato_loss as 
select 
distinct default.user_token,
sum(loss_cents/100) as loss_dllr
from default 
left join app_risk.app_risk.chargebacks cb
on default.user_token = cb.user_token
and payment_created_at <= default_date
and default.user_token not in (select user_token from app_risk.app_risk.ato_merchants)
and default.user_token not in (select user_token from app_risk.app_risk.fake_account_merchants)
group by 1
;

create or replace table seller_all_loss as
select default.*,loss.loss_dllr
from default
left join loss 
on default.user_token = loss.user_token
where loss_dllr > 10000
and default_date between '2019-01-01' and '2021-03-31'
;

create or replace table seller_no_fa_no_ato_loss as
select default.*, no_fa_no_ato_loss.loss_dllr
from default
left join no_fa_no_ato_loss 
on default.user_token = no_fa_no_ato_loss.user_token
where loss_dllr > 10000
and default_date between '2019-01-01' and '2021-03-31'
;

-- exposure calculation
create or replace table sellers_exposure_details as
select
sellers.user_token
,default_date
,du.business_type
,du.business_category
,cnp_presale_days
,cp_presale_days
,max(payment_trx_recognized_date) as max_trxn_dt
from seller_all_loss sellers
left join app_bi.pentagon.dim_user du
on sellers.user_token = du.user_token
left join fivetran.app_risk.policy_mcc_presale fv
on du.business_type = fv.business_type
left join app_bi.pentagon.aggregate_seller_daily_payment_summary dps
on sellers.user_token = dps.user_token
group by 1,2,3,4,5,6
;

-- final table
create or replace table final as
select 
seller.*
,NVL(SUM(CASE WHEN payment_trx_recognized_date > DATEADD(DAY, -cnp_presale_days, max_trxn_dt) AND payment_trx_recognized_date <= max_trxn_dt --DATEADD(DAY, -15, max_trxn_dt)
    THEN cnp_card_payment_amount_base_unit_usd / 100 END), 0) AS cnp_gpv
,NVL(SUM(CASE WHEN payment_trx_recognized_date > DATEADD(DAY, -cp_presale_days, max_trxn_dt) AND payment_trx_recognized_date <= max_trxn_dt --DATEADD(DAY, -15, max_trxn_dt)
    THEN cp_card_payment_amount_base_unit_usd / 100 END),0) AS cp_gpv
,cnp_gpv+cp_gpv as pre_sale_exposure
,case when pre_sale_exposure > 20000 then 1 else 0 end as exposure_gt_20k_flag
,case when pre_sale_exposure > 50000 then 1 else 0 end as exposure_gt_50k_flag
from sellers_exposure_details seller
left join app_bi.pentagon.aggregate_seller_daily_payment_summary dps
on seller.user_token = dps.unit_token
group by 1,2,3,4,5,6,7
;


select 
seller.user_token
,business_category
,seller.default_date
,os_dllr
,loss_dllr
,max_trxn_dt
,cnp_gpv
,cp_gpv
,pre_sale_exposure
,exposure_gt_20k_flag
,exposure_gt_50k_flag
--from seller_all_loss seller 
from seller_no_fa_no_ato_loss seller
left join final
on final.user_token = seller.user_token
;


