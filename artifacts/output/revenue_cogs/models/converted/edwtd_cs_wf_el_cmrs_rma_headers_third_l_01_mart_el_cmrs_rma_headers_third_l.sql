{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cmrs_rma_headers_third_l', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_EL_CMRS_RMA_HEADERS_THIRD_L',
        'target_table': 'EL_CMRS_RMA_HEADERS_THIRD_L',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.258189+00:00'
    }
) }}

WITH 

source_st_cmrs_rma_headers_third_l AS (
    SELECT
        batch_id,
        request_number,
        finance_director_email,
        finance_director_id,
        ges_update_date,
        global_name,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'st_cmrs_rma_headers_third_l') }}
),

final AS (
    SELECT
        request_number,
        finance_director_email,
        finance_director_id,
        ges_update_date,
        global_name,
        create_datetime
    FROM source_st_cmrs_rma_headers_third_l
)

SELECT * FROM final