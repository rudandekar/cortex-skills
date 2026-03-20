{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_functional_unit', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_FUNCTIONAL_UNIT',
        'target_table': 'W_FINANCIAL_FUNCTIONAL_UNIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.759515+00:00'
    }
) }}

WITH 

source_st_si_functional_unit AS (
    SELECT
        batch_id,
        functional_unit_id,
        functional_unit_value,
        functional_unit_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_functional_unit') }}
),

transformed_exp_wk_financial_functional_unit AS (
    SELECT
    bk_functional_unit_id,
    start_tv_dt,
    functional_unit_descr,
    sk_functional_unit_id_int,
    end_tv_dt,
    action_code,
    rank_index,
    dml_type
    FROM source_st_si_functional_unit
),

final AS (
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
    FROM transformed_exp_wk_financial_functional_unit
)

SELECT * FROM final