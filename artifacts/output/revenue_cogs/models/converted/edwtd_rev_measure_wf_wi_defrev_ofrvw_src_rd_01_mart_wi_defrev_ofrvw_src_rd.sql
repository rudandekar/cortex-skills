{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_ofrvw_src_rd', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_OFRVW_SRC_RD',
        'target_table': 'WI_DEFREV_OFRVW_SRC_RD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.055127+00:00'
    }
) }}

WITH 

source_wi_defrev_ofrvw_src_rd AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        bk_measure_name,
        l2_offer_type_name,
        xcat_flg,
        ela_flg,
        product_key,
        service_flg,
        product_subscription_flg,
        src_entity_name,
        recognized_rev_usd_amt,
        sales_territory_key,
        dv_attribution_cd,
        dv_product_key
    FROM {{ source('raw', 'wi_defrev_ofrvw_src_rd') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        bk_measure_name,
        l2_offer_type_name,
        xcat_flg,
        ela_flg,
        product_key,
        service_flg,
        product_subscription_flg,
        src_entity_name,
        recognized_rev_usd_amt,
        sales_territory_key,
        dv_attribution_cd,
        dv_product_key
    FROM source_wi_defrev_ofrvw_src_rd
)

SELECT * FROM final