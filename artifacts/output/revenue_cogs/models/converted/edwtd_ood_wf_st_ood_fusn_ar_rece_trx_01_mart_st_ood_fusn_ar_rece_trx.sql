{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_ar_rece_trx', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_AR_RECE_TRX',
        'target_table': 'ST_OOD_FUSN_AR_RECE_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.812777+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_ar_rece_trx AS (
    SELECT
        description,
        name,
        org_id,
        receivables_trx_id,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_ar_rece_trx') }}
),

final AS (
    SELECT
        description,
        name,
        org_id,
        receivables_trx_id,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_ar_rece_trx
)

SELECT * FROM final