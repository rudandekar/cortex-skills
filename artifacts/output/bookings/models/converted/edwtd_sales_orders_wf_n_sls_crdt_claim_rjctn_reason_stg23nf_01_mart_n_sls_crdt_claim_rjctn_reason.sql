{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sls_crdt_claim_rjctn_reason_stg23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SLS_CRDT_CLAIM_RJCTN_REASON_STG23NF',
        'target_table': 'N_SLS_CRDT_CLAIM_RJCTN_REASON',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.908590+00:00'
    }
) }}

WITH 

source_st_otm_gct_reasons AS (
    SELECT
        reason_code,
        claim_reason_name,
        reason_type,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_otm_gct_reasons') }}
),

final AS (
    SELECT
        bk_rejection_reason_cd,
        rejection_reason_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_otm_gct_reasons
)

SELECT * FROM final