{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_functional_unit_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_FUNCTIONAL_UNIT_TV',
        'target_table': 'N_FINANCIAL_FUNCTIONAL_UNIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.165083+00:00'
    }
) }}

WITH 

source_n_financial_functional_unit_tv AS (
    SELECT
        bk_functional_unit_id,
        start_tv_dt,
        end_tv_dt,
        functional_unit_descr,
        sk_functional_unit_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_financial_functional_unit_tv') }}
),

source_w_financial_functional_unit AS (
    SELECT
        bk_functional_unit_id,
        start_tv_dt,
        end_tv_dt,
        functional_unit_descr,
        sk_functional_unit_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_functional_unit') }}
),

final AS (
    SELECT
        bk_functional_unit_id,
        functional_unit_descr,
        sk_functional_unit_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_w_financial_functional_unit
)

SELECT * FROM final