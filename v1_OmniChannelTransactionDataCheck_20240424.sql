-- data verification for each channel on 2024-04-25, result = PASSED
-- edw.f_omni_channel_order_detail中已针对各渠道做以下处理
    -- LCS: data_source = 'lcs-pos-order-detail' and partner_name <> 'LBR' and if_package_tag = false
    -- LCS: inner join edw.d_dl_product_info_latest to exclude self-built merchandise
    -- TM: is_gwp_via_gmv = 'N' and lego_sku_gmv_price>= 59 and lego_sku_rrp_price > 0 and platformid = 'taobao'
    -- DY: is_gwp_via_gmv = 'N' and platformid = 'douyin'
-- key metrics description
    -- sales_type: DOUYIN = null, DOUYIN_B2B = null, TMALL = null, LCS = [0, 6]
    -- if_eff_order_tag: DOUYIN = true, DOUYIN_B2B = true, TMALL = true, LCS = (ture, false)
    -- source_channel = DOUYIN, DOUYIN_B2B, TMALL, LCS
    -- crm_member_detail_id: 就是member_detail_id
    -- type_name: 渠道内的id类型，LCS = CRM_memberid (9 digits)， DOUYIN & DOUYIN_B2B = DY_openid, TM = TMALL_kyid
    -- type_value: 对应type_name的具体id数值

-- LCS Part
with
lego_calendar_fact as
    (
        select
        date_id,
        lego_year,
        lego_month,
        lego_week,
        d_day_of_lego_week
        from edw.d_dl_calendar
        where 1 = 1
        and date_type = 'day'
        and date_id < current_date
    )

select
'current' :: text as caliber,
cm1.lego_year as trans_lego_year,

sum(case when sales_qty > 0 then order_rrp_amt else 0 end) as total_normal_sales,
sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as toral_return_sales,
total_normal_sales - toral_return_sales as valid_total_sales,
count(distinct case when if_eff_order_tag = true then concat(original_store_code, original_order_id) else null end) as valid_total_order,

sum(case when sales_qty > 0 then sales_qty else 0 end) as total_normal_qty,
sum(case when sales_qty < 0 then abs(sales_qty) else 0 end) as toral_return_qty,
total_normal_qty - toral_return_qty as valid_total_qty,

sum(case when sales_qty > 0 and crm_member_id is not null then order_rrp_amt else 0 end) as member_normal_sales,
sum(case when sales_qty < 0 and crm_member_id is not null then abs(order_rrp_amt) else 0 end) as member_return_sales,
member_normal_sales - member_return_sales as valid_member_sales,
count(distinct case when if_eff_order_tag = true and crm_member_id is not null then concat(original_store_code, original_order_id) else null end) as valid_member_order,
count(distinct case when if_eff_order_tag = true then crm_member_id else null end) as valid_member_shopper,

sum(case when sales_qty > 0 and crm_member_id is not null then sales_qty else 0 end) as member_normal_qty,
sum(case when sales_qty < 0 and crm_member_id is not null then abs(sales_qty) else 0 end) as member_return_qty,
member_normal_qty - member_return_qty as valid_member_qty,

sum(case when sales_qty > 0 and crm_member_id is not null and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then order_rrp_amt else 0 end) as new_member_normal_sales,
sum(case when sales_qty < 0 and crm_member_id is not null and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then abs(order_rrp_amt) else 0 end) as new_member_return_sales,
new_member_normal_sales - new_member_return_sales as valid_new_member_sales,
count(distinct case when if_eff_order_tag = true and crm_member_id is not null and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then concat(original_store_code, original_order_id) else null end) as valid_new_member_order,
count(distinct case when if_eff_order_tag = true and crm_member_id is not null and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then crm_member_id else null end) as valid_new_member_shopper,

sum(case when sales_qty > 0 and crm_member_id is not null and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then sales_qty else 0 end) as new_member_normal_qty,
sum(case when sales_qty < 0 and crm_member_id is not null and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then abs(sales_qty) else 0 end) as new_member_return_qty,
new_member_normal_qty - new_member_return_qty as valid_new_member_qty

from edw.f_lcs_order_detail as tr
inner join edw.d_dl_product_info_latest as pi
on tr.lego_sku_id = pi.lego_sku_id
left join edw.f_crm_member_detail as mbr
on tr.crm_member_id = mbr.member_id
left join edw.d_member_detail as reg
on mbr.id = reg.member_detail_id
left join lego_calendar_fact as cm1
on date(tr.date_id) = date(cm1.date_id)
left join lego_calendar_fact as cm2
on date(reg.join_date) = date(cm2.date_id)
where 1 = 1
and tr.date_id < '2024-04-25'
and (is_gwp = false or sales_type = 2)
and data_source = 'lcs-pos-order-detail'
and partner_name <> 'LBR'
and sales_type <> 3
and if_package_tag = false
group by
cm1.lego_year

union all

select
'new' :: text as caliber,
cm1.lego_year as trans_lego_year,

sum(case when sales_qty > 0 then order_rrp_amt else 0 end) as total_normal_sales,
sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as toral_return_sales,
total_normal_sales - toral_return_sales as valid_total_sales,
count(distinct case when if_eff_order_tag = true then concat(original_store_code, original_order_id) else null end) as valid_total_order,

sum(case when sales_qty > 0 then sales_qty else 0 end) as total_normal_qty,
sum(case when sales_qty < 0 then abs(sales_qty) else 0 end) as toral_return_qty,
total_normal_qty - toral_return_qty as valid_total_qty,

sum(case when sales_qty > 0 and is_member_order = true then order_rrp_amt else 0 end) as member_normal_sales,
sum(case when sales_qty < 0 and is_member_order = true then abs(order_rrp_amt) else 0 end) as member_return_sales,
member_normal_sales - member_return_sales as valid_member_sales,
count(distinct case when if_eff_order_tag = true and is_member_order = true then concat(original_store_code, original_order_id) else null end) as valid_member_order,
count(distinct case when if_eff_order_tag = true and is_member_order = true then type_value else null end) as valid_member_shopper,

sum(case when sales_qty > 0 and is_member_order = true then sales_qty else 0 end) as member_normal_qty,
sum(case when sales_qty < 0 and is_member_order = true then abs(sales_qty) else 0 end) as member_return_qty,
member_normal_qty - member_return_qty as valid_member_qty,

sum(case when sales_qty > 0 and is_member_order = true and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then order_rrp_amt else 0 end) as new_member_normal_sales,
sum(case when sales_qty < 0 and is_member_order = true and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then abs(order_rrp_amt) else 0 end) as new_member_return_sales,
new_member_normal_sales - new_member_return_sales as valid_new_member_sales,
count(distinct case when if_eff_order_tag = true and is_member_order = true and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then concat(original_store_code, original_order_id) else null end) as valid_new_member_order,
count(distinct case when if_eff_order_tag = true and is_member_order = true and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then type_value else null end) as valid_new_member_shopper,

sum(case when sales_qty > 0 and is_member_order = true and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then sales_qty else 0 end) as new_member_normal_qty,
sum(case when sales_qty < 0 and is_member_order = true and reg.eff_reg_channel like '%LCS%' and cm1.lego_year = cm2.lego_year then abs(sales_qty) else 0 end) as new_member_return_qty,
new_member_normal_qty - new_member_return_qty as valid_new_member_qty

from edw.f_omni_channel_order_detail as tr
left join edw.f_crm_member_detail as mbr
on tr.crm_member_detail_id = mbr.member_id
left join edw.d_member_detail as reg
on mbr.id = reg.member_detail_id
left join lego_calendar_fact as cm1
on date(tr.order_paid_date) = date(cm1.date_id)
left join lego_calendar_fact as cm2
on date(reg.join_date) = date(cm2.date_id)
where 1 = 1
and tr.order_paid_date < '2024-04-25'
and source_channel = 'LCS'
and sales_type <> 3 -- criteria for RRP sales in LCS
group by
cm1.lego_year

-----------------------

-- TM part
with
lego_calendar_fact as
    (
        select
        date_id,
        lego_year,
        lego_month,
        lego_week,
        d_day_of_lego_week
        from edw.d_dl_calendar
        where 1 = 1
        and date_type = 'day'
        and date_id < current_date
    )

select
'current' :: text as caliber,
cm1.lego_year,

sum(tr.order_rrp_amount ) as total_sales,
count(distinct parent_order_id) as total_order,
count(distinct tr.kyid) as total_shopper,
sum(tr.piece_cnt) as total_qty,

sum(case when tr.is_member = 1 then tr.order_rrp_amount  else 0 end) as member_sales,
count(distinct case when tr.is_member = 1 then tr.parent_order_id else null end) as member_order,
count(distinct case when tr.is_member = 1 then tr.kyid else null end) as member_shopper,
sum(case when tr.is_member = 1 then tr.piece_cnt else 0 end) as member_qty,

sum(case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.order_rrp_amount  else 0 end) as new_member_sales,
count(distinct case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.parent_order_id else null end) as new_member_order,
count(distinct case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.kyid else null end) as new_member_shopper,
sum(case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.piece_cnt else 0 end) as new_member_qty

from edw.f_oms_order_dtl_upd as tr
left join lego_calendar_fact as cm1
on date(tr.payment_confirm_time) = date(cm1.date_id)
left join lego_calendar_fact as cm2
on date(tr.first_bind_time) = date(cm2.date_id)
where 1 = 1
and is_delivered = 'Y'
and is_gwp_via_gmv = 'N'
and lego_sku_gmv_price>= 59
and lego_sku_rrp_price > 0
and platformid = 'taobao'
and date(payment_confirm_time) < '2024-04-25'
group by
cm1.lego_year

union all

select
'new' :: text as caliber,
cm1.lego_year,

sum(tr.order_rrp_amt) as total_sales,
count(distinct parent_order_id) as total_order,
count(distinct tr.type_value) as total_shopper,
sum(tr.sales_qty) as total_qty,

sum(case when tr.is_member_order = true then tr.order_rrp_amt  else 0 end) as member_sales,
count(distinct case when tr.is_member_order = true then tr.parent_order_id else null end) as member_order,
count(distinct case when tr.is_member_order = true then tr.type_value else null end) as member_shopper,
sum(case when tr.is_member_order = true then tr.sales_qty else 0 end) as member_qty,

sum(case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.order_rrp_amt  else 0 end) as new_member_sales,
count(distinct case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.parent_order_id else null end) as new_member_order,
count(distinct case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.type_value else null end) as new_member_shopper,
sum(case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.sales_qty else 0 end) as new_member_qty

from edw.f_omni_channel_order_detail as tr
left join lego_calendar_fact as cm1
on date(tr.order_paid_date) = date(cm1.date_id)
left join lego_calendar_fact as cm2
on date(tr.first_bind_time) = date(cm2.date_id)
where 1 = 1
and date(tr.order_paid_date) < '2024-04-25'
and tr.source_channel = 'TMALL'
and tr.order_type = 'normal' -- criteria for sales in TM
group by
cm1.lego_year

-----------------------

-- DY Part
with
lego_calendar_fact as
    (
        select
        date_id,
        lego_year,
        lego_month,
        lego_week,
        d_day_of_lego_week
        from edw.d_dl_calendar
        where 1 = 1
        and date_type = 'day'
        and date_id < current_date
    ),

dy_sales_mid as
    (
        -- B2C logic
        select
        douyin_openid,
        date(payment_confirm_time) as date_id,
        member_detail_id as crm_member_detail_id,
        parent_order_id,
        order_gmv_amount as order_gmv_amount,
        order_rrp_amount as order_rrp_amount,
        piece_cnt,
        lego_sku_id,
        shopid,
        case
        when shopid = 'LEGO乐高抖音官方旗舰店' then 'dy-brand-store'
        when shopid = 'LEGO乐高抖音亲子店旗舰店' then 'dy-family-store'
        else 'exception' end as eff_shopid,
        first_bind_time,
        is_member,
        case when date(first_bind_time) <= date(payment_confirm_time) then 1 else 0 end as is_member_eff
        from edw.f_oms_order_dtl_upd
        where 1 = 1
        and is_delivered = 'Y'
        and is_gwp_via_gmv = 'N'
        and platformid = 'douyin'
        and date(payment_confirm_time) < current_date

        union all

        -- B2B logic
        select
        douyin_openid,
        date(payment_confirm_time) as date_id,
        member_detail_id as crm_member_detail_id,
        parent_order_id,
        order_gmv_amount as order_gmv_amount,
        order_rrp_amount as order_rrp_amount,
        piece_cnt,
        lego_sku_id,
        shopid,
        case
        when shopid in ('抖音乐高旗舰店', 'LEGO乐高抖音官方旗舰店', 'LEGO乐高官方旗舰店') then 'dy-brand-store'
        when shopid in ('抖音亲子官方旗舰店', 'LEGO乐高抖音亲子店旗舰店', 'LEGO乐高亲子旗舰店') then 'dy-family-store'
        else 'exception' end as eff_shopid,
        first_bind_time,
        is_member,
        case when date(first_bind_time) <= date(payment_confirm_time) then 1 else 0 end as is_member_eff
        from edw.f_oms_order_dtl_dy_b2b
        where 1 = 1
        and is_delivered = 'Y'
        and is_gwp_via_gmv = 'N'
        and platformid = 'douyin'
        and date(payment_confirm_time) < current_date
    )

select
'current' :: text as caliber,
cm1.lego_year,

sum(tr.order_rrp_amount ) as total_sales,
count(distinct tr.parent_order_id) as total_order,
count(distinct tr.douyin_openid) as total_shopper,
sum(tr.piece_cnt) as total_qty,

sum(case when tr.is_member = 1 then tr.order_rrp_amount  else 0 end) as member_sales,
count(distinct case when tr.is_member = 1 then tr.parent_order_id else null end) as member_order,
count(distinct case when tr.is_member = 1 then tr.douyin_openid else null end) as member_shopper,
sum(case when tr.is_member = 1 then tr.piece_cnt else 0 end) as member_qty,

sum(case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.order_rrp_amount  else 0 end) as new_member_sales,
count(distinct case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.parent_order_id else null end) as new_member_order,
count(distinct case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.douyin_openid else null end) as new_member_shopper,
sum(case when tr.is_member = 1 and cm1.lego_year = cm2.lego_year then tr.piece_cnt else 0 end) as new_member_qty

from dy_sales_mid as tr
left join lego_calendar_fact as cm1
on tr.date_id = date(cm1.date_id)
left join lego_calendar_fact as cm2
on date(tr.first_bind_time) = date(cm2.date_id)
where 1 = 1
and tr.date_id < '2024-04-25'
group by
cm1.lego_year

union all

select
'new' :: text as caliber,
cm1.lego_year,

sum(tr.order_rrp_amt) as total_sales,
count(distinct parent_order_id) as total_order,
count(distinct tr.type_value) as total_shopper,
sum(tr.sales_qty) as total_qty,

sum(case when tr.is_member_order = true then tr.order_rrp_amt  else 0 end) as member_sales,
count(distinct case when tr.is_member_order = true then tr.parent_order_id else null end) as member_order,
count(distinct case when tr.is_member_order = true then tr.type_value else null end) as member_shopper,
sum(case when tr.is_member_order = true then tr.sales_qty else 0 end) as member_qty,

sum(case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.order_rrp_amt  else 0 end) as new_member_sales,
count(distinct case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.parent_order_id else null end) as new_member_order,
count(distinct case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.type_value else null end) as new_member_shopper,
sum(case when tr.is_member_order = true and cm1.lego_year = cm2.lego_year then tr.sales_qty else 0 end) as new_member_qty

from edw.f_omni_channel_order_detail as tr
left join lego_calendar_fact as cm1
on date(tr.order_paid_date) = date(cm1.date_id)
left join lego_calendar_fact as cm2
on date(tr.first_bind_time) = date(cm2.date_id)
where 1 = 1
and date(tr.order_paid_date) < '2024-04-25'
and tr.source_channel in ('DOUYIN', 'DOUYIN_B2B')
and tr.order_type = 'normal' -- criteria for sales in DY
group by
cm1.lego_year