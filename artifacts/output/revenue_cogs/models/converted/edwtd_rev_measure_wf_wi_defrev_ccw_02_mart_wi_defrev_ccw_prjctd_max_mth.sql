{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_ccw', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_CCW',
        'target_table': 'WI_DEFREV_CCW_PRJCTD_MAX_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.695102+00:00'
    }
) }}

WITH 

source_wi_defrev_ccw_discont_mth AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_ccw_discont_mth') }}
),

source_wi_defrev_ccw AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_ccw') }}
),

source_wi_defrev_ccw_prjctd_max_mth AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        bk_deal_id,
        fiscal_year_month_int,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_ccw_prjctd_max_mth') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        bk_deal_id,
        fiscal_year_month_int,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM source_wi_defrev_ccw_prjctd_max_mth
)

SELECT * FROM final