{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_linked_sol_nos', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_LINKED_SOL_NOS',
        'target_table': 'ST_Linked_SOL_NOS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.145446+00:00'
    }
) }}

WITH 

source_st_linked_sol_nos AS (
    SELECT
        line_id,
        linked_so_ln_nums,
        edw_create_dtm
    FROM {{ source('raw', 'st_linked_sol_nos') }}
),

transformed_exptrans AS (
    SELECT
    line_id,
    linked_so_ln_nums,
    edw_create_dtm
    FROM source_st_linked_sol_nos
),

final AS (
    SELECT
        line_id,
        linked_so_ln_nums,
        edw_create_dtm
    FROM transformed_exptrans
)

SELECT * FROM final