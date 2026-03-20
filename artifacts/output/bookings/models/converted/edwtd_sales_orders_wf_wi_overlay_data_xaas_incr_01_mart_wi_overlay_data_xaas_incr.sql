{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_overlay_data_xaas_incr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_OVERLAY_DATA_XAAS_INCR',
        'target_table': 'WI_OVERLAY_DATA_XAAS_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.192601+00:00'
    }
) }}

WITH 

source_wi_overlay_data_xaas_incr AS (
    SELECT
        sk_trx_sc_id_int,
        sls_credit_asgnmt_sls_trx_key,
        bk_sls_terr_assignment_type_cd,
        dv_sca_create_dt,
        bk_sales_rep_num,
        pd_sales_territory_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        split_pct,
        effective_start_dtm,
        effective_end_dtm,
        src_edw_create_dtm,
        src_edw_update_dtm,
        edw_create_dtm,
        edw_update_dtm,
        sca_last_update_dtm,
        pd_sales_credit_usage_cd,
        sk_offer_attribution_id_int,
        rtnr_unique_id,
        line_creation_date,
        src_indicator,
        pd_sales_commission_pct_orig
    FROM {{ source('raw', 'wi_overlay_data_xaas_incr') }}
),

final AS (
    SELECT
        sk_trx_sc_id_int,
        sls_credit_asgnmt_sls_trx_key,
        bk_sls_terr_assignment_type_cd,
        dv_sca_create_dt,
        bk_sales_rep_num,
        pd_sales_territory_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        split_pct,
        effective_start_dtm,
        effective_end_dtm,
        src_edw_create_dtm,
        src_edw_update_dtm,
        edw_create_dtm,
        edw_update_dtm,
        sca_last_update_dtm,
        pd_sales_credit_usage_cd,
        sk_offer_attribution_id_int,
        rtnr_unique_id,
        line_creation_date,
        src_indicator,
        pd_sales_commission_pct_orig
    FROM source_wi_overlay_data_xaas_incr
)

SELECT * FROM final