-- edw.f_omni_channel_order_detail中已针对各渠道做以下处理
    -- LCS: data_source = 'lcs-pos-order-detail' and partner_name <> 'LBR' and if_package_tag = false
    -- TM: is_gwp_via_gmv = 'N' and lego_sku_gmv_price>= 59 and lego_sku_rrp_price > 0 and platformid = 'taobao'
    -- DY: is_gwp_via_gmv = 'N' and platformid = 'douyin'
    -- sales_type: DOUYIN = null, DOUYIN_B2B = null, TMALL = null, LCS = [0, 6]
    -- if_eff_order_tag: DOUYIN = true, DOUYIN_B2B = true, TMALL = true, LCS = (ture, false)
    -- source_channel = DOUYIN, DOUYIN_B2B, TMALL, LCS

select
from edw.f_omni_channel_order_detail
where 1 = 1
and sales_type <> 3 -- DOUYIN = null, DOUYIN_B2B = null, TMALL = null, LCS = [0, 6]