{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_rev_drvd_rae_revenue_rao', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_REV_DRVD_RAE_REVENUE_RAO',
        'target_table': 'WI_AUD_AWI_DRVD_RAE_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.210740+00:00'
    }
) }}

WITH 

source_wi_aud_drvd_rae_rev_rao_tmp AS (
    SELECT
        bk_ar_trx_number,
        ar_trx_datetime,
        ar_trx_key,
        ru_bk_adjd_ar_trx_key,
        ar_trx_line_key,
        net_revenue_key,
        edw_create_datetime,
        audit_job_name,
        aud_count
    FROM {{ source('raw', 'wi_aud_drvd_rae_rev_rao_tmp') }}
),

transformed_exp_counts_mismatch AS (
    SELECT
    bk_ar_trx_number,
    ar_trx_datetime,
    ar_trx_key,
    ru_bk_adjd_ar_trx_key,
    ar_trx_line_key,
    net_revenue_key,
    edw_create_datetime,
    audit_job_name,
    aud_count,
    IFF(AUD_COUNT!=0,ABORT('Record counts are not matching')) AS abort
    FROM source_wi_aud_drvd_rae_rev_rao_tmp
),

final AS (
    SELECT
        audit_job_name,
        bk_ar_trx_number,
        ar_trx_datetime,
        ar_trx_key,
        ru_bk_adjd_ar_trx_key,
        ar_trx_line_key,
        net_revenue_key,
        edw_create_datetime
    FROM transformed_exp_counts_mismatch
)

SELECT * FROM final