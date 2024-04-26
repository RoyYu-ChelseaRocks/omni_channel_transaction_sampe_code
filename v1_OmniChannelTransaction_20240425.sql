with
lego_calendar_fact as
    (
        select
        date_id,
        lego_year,
        lego_month,
        lego_week,
        lego_quarter,
        d_day_of_lego_week
        from edw.d_dl_calendar
        where 1 = 1
        and date_type = 'day'
        and date_id < current_date
    ),

omni_trans_fact as
    (
        select
        date(tr.order_paid_time) as order_paid_date,
        case
        when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar))
        else null end as omni_channel_member_id, -- 优先取member_detail_id，缺失情况下再取渠道内部id
        tr.parent_order_id,
        tr.sales_qty, -- 用于为LCS判断正负单
        tr.if_eff_order_tag, -- 该字段仅对LCS有true / false之分，对于其余渠道均为true
        tr.is_member_order,
        tr.order_rrp_amt,
        cm1.lego_year as trans_lego_year,
        cm1.lego_quarter as trans_lego_quarter,
        cm1.lego_month as trans_lego_month,
        cm2.lego_year as reg_lego_year,
        cm2.lego_quarter as reg_lego_quarter,
        cm2.lego_month as reg_lego_month     
        from edw.f_omni_channel_order_detail as tr
        left join edw.f_crm_member_detail as mbr
        on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
        left join lego_calendar_fact as cm1 -- cm1 for mapping of transaction date
        on date(tr.order_paid_time) = date(cm1.date_id)
        left join lego_calendar_fact as cm2 -- cm1 for mapping of registration date
        on coalesce(date(mbr.join_time), date(tr.first_bind_time)) = date(cm2.date_id) -- 优先取CRM注册时间，缺失情况下取渠道内绑定时间
        where 1 = 1
        and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
        and date(tr.order_paid_time) < current_date
        and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
    )

-----------------------

select
sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as total_sales,
sum(case when sales_qty > 0 and is_member_order = true then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 and is_member_order = true then abs(order_rrp_amt) else 0 end) as member_sales,
count(distinct case when is_member_order = true and if_eff_order_tag = true then omni_channel_member_id else null end) as member_shopper,
count(distinct case when is_member_order = true and if_eff_order_tag = true then parent_order_id else null end) as member_order,
sum(case when sales_qty > 0 and is_member_order = true and trans_lego_year = reg_lego_year then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 and is_member_order = true and trans_lego_year = reg_lego_year then abs(order_rrp_amt) else 0 end) as new_member_sales,
count(distinct case when is_member_order = true and if_eff_order_tag = true and trans_lego_year = reg_lego_year then omni_channel_member_id else null end) as new_member_shopper,
count(distinct case when is_member_order = true and if_eff_order_tag = true and trans_lego_year = reg_lego_year then parent_order_id else null end) as new_member_order,
member_sales - new_member_sales as existing_member_sales,
member_shopper - new_member_shopper as existing_member_shopper,
member_order - new_member_order as existing_member_order
from omni_trans_fact
where 1 = 1
and order_paid_date >= '2024-01-01'
and order_paid_date < '2024-04-25'