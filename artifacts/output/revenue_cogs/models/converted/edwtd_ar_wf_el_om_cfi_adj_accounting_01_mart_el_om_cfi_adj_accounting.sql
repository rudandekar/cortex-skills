{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_om_cfi_adj_accounting', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_OM_CFI_ADJ_ACCOUNTING',
        'target_table': 'EL_OM_CFI_ADJ_ACCOUNTING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.117097+00:00'
    }
) }}

WITH 

source_el_om_cfi_adj_accounting AS (
    SELECT
        accountno,
        adj_accounting_id,
        account_type,
        adj_type_id,
        global_name
    FROM {{ source('raw', 'el_om_cfi_adj_accounting') }}
),

source_st_om_cfi_adj_accounting AS (
    SELECT
        batch_id,
        accountno,
        adj_accounting_id,
        account_type,
        adj_type_id,
        subaccount,
        global_name,
        creation_date,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_adj_accounting') }}
),

final AS (
    SELECT
        accountno,
        adj_accounting_id,
        account_type,
        adj_type_id,
        global_name
    FROM source_st_om_cfi_adj_accounting
)

SELECT * FROM final