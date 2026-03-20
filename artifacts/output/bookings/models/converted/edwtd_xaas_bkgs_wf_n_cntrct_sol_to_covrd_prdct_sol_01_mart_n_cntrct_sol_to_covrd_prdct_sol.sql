{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cntrct_sol_to_covrd_prdct_sol', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_CNTRCT_SOL_TO_COVRD_PRDCT_SOL',
        'target_table': 'N_CNTRCT_SOL_TO_COVRD_PRDCT_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.863523+00:00'
    }
) }}

WITH 

source_n_cntrct_sol_to_covrd_prdct_sol AS (
    SELECT
        covered_product_sol_key,
        service_contract_sol_key,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_cntrct_sol_to_covrd_prdct_sol') }}
),

transformed_exptrans AS (
    SELECT
    covered_product_sol_key,
    service_contract_sol_key,
    edw_create_user,
    edw_update_user,
    edw_create_datetime,
    edw_update_datetime
    FROM source_n_cntrct_sol_to_covrd_prdct_sol
),

final AS (
    SELECT
        covered_product_sol_key,
        service_contract_sol_key,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM transformed_exptrans
)

SELECT * FROM final