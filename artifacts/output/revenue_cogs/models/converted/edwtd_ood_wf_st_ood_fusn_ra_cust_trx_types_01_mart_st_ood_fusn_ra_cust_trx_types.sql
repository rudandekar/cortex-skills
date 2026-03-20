{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_ra_cust_trx_types', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_RA_CUST_TRX_TYPES',
        'target_table': 'ST_OOD_FUSN_RA_CUST_TRX_TYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.188449+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_ra_cust_trx_types AS (
    SELECT
        cust_trx_type_id,
        description,
        name,
        org_id,
        type_code,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_ra_cust_trx_types') }}
),

final AS (
    SELECT
        cust_trx_type_id,
        description,
        name,
        org_id,
        type_code,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_ra_cust_trx_types
)

SELECT * FROM final