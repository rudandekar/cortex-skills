{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rev_sca_nrt_xaas_tv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_REV_SCA_NRT_XAAS_TV',
        'target_table': 'WI_REV_SCA_NRT_XAAS_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.653518+00:00'
    }
) }}

WITH 

source_wi_rev_sca_nrt_xaas_tv AS (
    SELECT
        sls_credit_asgnmt_sls_trx_key,
        sls_credit_asgnmt_mode_cd,
        sk_trx_sc_id_int,
        bk_sales_rep_num,
        bk_sls_terr_asgnmt_type_cd,
        so_sbscrptn_itm_sls_trx_key,
        sales_territory_key,
        bk_sales_credit_type_code,
        split_pct,
        sca_source_type_cd,
        effective_start_dtm,
        effective_end_dtm,
        source_commit_dtm,
        edw_create_dtm,
        edw_update_dtm,
        quota_flag,
        ar_trx_line_key,
        sbscrptn_item_change_id_int,
        action_cd,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        dv_fiscal_year_month_num_int,
        event_type
    FROM {{ source('raw', 'wi_rev_sca_nrt_xaas_tv') }}
),

source_wi_rev_sca_nrt_xaas_incr AS (
    SELECT
        sls_credit_asgnmt_sls_trx_key,
        sls_credit_asgnmt_mode_cd,
        sk_trx_sc_id_int,
        bk_sales_rep_num,
        bk_sls_terr_asgnmt_type_cd,
        so_sbscrptn_itm_sls_trx_key,
        sales_territory_key,
        bk_sales_credit_type_code,
        split_pct,
        sca_source_type_cd,
        effective_start_dtm,
        effective_end_dtm,
        source_commit_dtm,
        edw_create_dtm,
        edw_update_dtm,
        quota_flag,
        ar_trx_line_key,
        sbscrptn_item_change_id_int,
        action_cd,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        dv_fiscal_year_month_num_int,
        event_type
    FROM {{ source('raw', 'wi_rev_sca_nrt_xaas_incr') }}
),

final AS (
    SELECT
        sls_credit_asgnmt_sls_trx_key,
        sls_credit_asgnmt_mode_cd,
        sk_trx_sc_id_int,
        bk_sales_rep_num,
        bk_sls_terr_asgnmt_type_cd,
        so_sbscrptn_itm_sls_trx_key,
        sales_territory_key,
        bk_sales_credit_type_code,
        split_pct,
        sca_source_type_cd,
        effective_start_dtm,
        effective_end_dtm,
        source_commit_dtm,
        edw_create_dtm,
        edw_update_dtm,
        quota_flag,
        ar_trx_line_key,
        sbscrptn_item_change_id_int,
        action_cd,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        dv_fiscal_year_month_num_int,
        event_type
    FROM source_wi_rev_sca_nrt_xaas_incr
)

SELECT * FROM final