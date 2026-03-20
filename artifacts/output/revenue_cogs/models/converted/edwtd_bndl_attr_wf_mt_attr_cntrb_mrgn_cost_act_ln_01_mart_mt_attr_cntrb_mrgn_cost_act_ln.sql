{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_attr_cntrb_mrgn_cost_act_ln', 'batch', 'edwtd_bndl_attr'],
    meta={
        'source_workflow': 'wf_m_MT_ATTR_CNTRB_MRGN_COST_ACT_LN',
        'target_table': 'MT_ATTR_CNTRB_MRGN_COST_ACT_LN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.633473+00:00'
    }
) }}

WITH 

source_wi_cntrb_mrgn_non_bundle AS (
    SELECT
        cntrb_mrgn_cost_actl_ln_key,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_product_key,
        cntrb_mrgn_actl_ln_usd_amt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'wi_cntrb_mrgn_non_bundle') }}
),

source_wi_cntrb_mrgn_bundle AS (
    SELECT
        cntrb_mrgn_cost_actl_ln_key,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_product_key,
        cntrb_mrgn_actl_ln_usd_amt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        bk_handshake_bundle_type_name
    FROM {{ source('raw', 'wi_cntrb_mrgn_bundle') }}
),

final AS (
    SELECT
        cntrb_mrgn_cost_actl_ln_key,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_product_key,
        cntrb_mrgn_actl_ln_usd_amt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        attributed_flg,
        bundle_product_key,
        bk_handshake_bundle_type_name
    FROM source_wi_cntrb_mrgn_bundle
)

SELECT * FROM final