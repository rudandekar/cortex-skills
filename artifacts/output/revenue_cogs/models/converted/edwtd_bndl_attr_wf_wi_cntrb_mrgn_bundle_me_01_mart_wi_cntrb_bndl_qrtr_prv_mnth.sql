{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cntrb_mrgn_bundle_me', 'batch', 'edwtd_bndl_attr'],
    meta={
        'source_workflow': 'wf_m_WI_CNTRB_MRGN_BUNDLE_ME',
        'target_table': 'WI_CNTRB_BNDL_QRTR_PRV_MNTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.483261+00:00'
    }
) }}

WITH 

source_wi_cntrb_curr_last_qrtr AS (
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
    FROM {{ source('raw', 'wi_cntrb_curr_last_qrtr') }}
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
        bk_handshake_bundle_type_name
    FROM source_wi_cntrb_curr_last_qrtr
)

SELECT * FROM final