{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_fin_dfr_as_vert', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_FIN_DFR_AS_VERT',
        'target_table': 'WI_AS_FIN_DFR_RSTD_VERT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.632113+00:00'
    }
) }}

WITH 

source_wi_as_fin_dfr_rstd_vert AS (
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
        dfrev_as_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        rev_measurement_type_cd,
        transaction_seq_id,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_as_fin_dfr_rstd_vert') }}
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
        dfrev_as_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        rev_measurement_type_cd,
        transaction_seq_id,
        dv_recurring_offer_cd
    FROM source_wi_as_fin_dfr_rstd_vert
)

SELECT * FROM final