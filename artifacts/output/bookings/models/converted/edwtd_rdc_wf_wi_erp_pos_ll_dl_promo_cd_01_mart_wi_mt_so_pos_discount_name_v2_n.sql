{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_erp_pos_ll_dl_promo_cd', 'batch', 'edwtd_rdc'],
    meta={
        'source_workflow': 'wf_m_WI_ERP_POS_LL_DL_PROMO_CD',
        'target_table': 'WI_MT_SO_POS_DISCOUNT_NAME_V2_N',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.856243+00:00'
    }
) }}

WITH 

source_wi_so_pos_dl_promo_cd_int1 AS (
    SELECT
        bk_deal_id,
        bk_promotion_cd,
        rank_1
    FROM {{ source('raw', 'wi_so_pos_dl_promo_cd_int1') }}
),

source_wi_so_pos_ll_names AS (
    SELECT
        bk_deal_id,
        product_id,
        list_of_promotion_line_level,
        base_discount_name_line_level,
        non_standard_name_line_level,
        ctr_flg
    FROM {{ source('raw', 'wi_so_pos_ll_names') }}
),

source_wi_so_pos_discount_name AS (
    SELECT
        bk_deal_id,
        product_id,
        promotion_name,
        discount_name,
        source_reported_promotion_cd,
        discount_percent,
        discount_amount,
        active_yorn,
        hpefix_yorn
    FROM {{ source('raw', 'wi_so_pos_discount_name') }}
),

source_wi_so_pos_ll_promo_cd AS (
    SELECT
        bk_deal_id,
        product_id,
        list_of_promotion_line_level,
        bk_promotion_cd
    FROM {{ source('raw', 'wi_so_pos_ll_promo_cd') }}
),

source_wi_so_pos_ll_promo_cd_int1 AS (
    SELECT
        bk_deal_id,
        product_id,
        bk_promotion_cd,
        rank_1
    FROM {{ source('raw', 'wi_so_pos_ll_promo_cd_int1') }}
),

final AS (
    SELECT
        bk_deal_id,
        product_id,
        discount_name,
        promotion_code,
        discount_percent,
        discount_amount
    FROM source_wi_so_pos_ll_promo_cd_int1
)

SELECT * FROM final