{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_fin_dfrev_tss_flat', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_FIN_DFREV_TSS_FLAT',
        'target_table': 'WI_FIN_DFREV_TSS_FLAT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.984593+00:00'
    }
) }}

WITH 

source_wi_fin_dfrev_tss_flat AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        dfrev_tss_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_recurring_offer_cd,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type
    FROM {{ source('raw', 'wi_fin_dfrev_tss_flat') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        dfrev_tss_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_recurring_offer_cd,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type
    FROM source_wi_fin_dfrev_tss_flat
)

SELECT * FROM final