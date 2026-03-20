{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_cntrct_sol_to_covrd_prdct_sol', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_CNTRCT_SOL_TO_COVRD_PRDCT_SOL',
        'target_table': 'ST_Linked_SOL_NOS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.048996+00:00'
    }
) }}

WITH 

source_ex_linked_sol_nos AS (
    SELECT
        line_id,
        linked_so_ln_nums,
        edw_create_dtm
    FROM {{ source('raw', 'ex_linked_sol_nos') }}
),

source_w_cntrct_sol_to_covrd_prdct_sol AS (
    SELECT
        covered_product_sol_key,
        service_contract_sol_key,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cntrct_sol_to_covrd_prdct_sol') }}
),

source_st_linked_sol_nos AS (
    SELECT
        line_id,
        linked_so_ln_nums,
        edw_create_dtm
    FROM {{ source('raw', 'st_linked_sol_nos') }}
),

transformed_exptrans AS (
    SELECT
    covered_product_sol_key,
    service_contract_sol_key,
    edw_create_user,
    edw_update_user,
    edw_create_datetime,
    edw_update_datetime,
    action_code,
    dml_type
    FROM source_st_linked_sol_nos
),

transformed_exptrans1 AS (
    SELECT
    line_id,
    linked_so_ln_nums,
    edw_create_dtm
    FROM transformed_exptrans
),

transformed_exptrans2 AS (
    SELECT
    line_id,
    linked_so_ln_nums,
    edw_create_dtm
    FROM transformed_exptrans1
),

final AS (
    SELECT
        line_id,
        linked_so_ln_nums,
        edw_create_dtm
    FROM transformed_exptrans2
)

SELECT * FROM final