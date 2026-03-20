{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_fin_acct_locality', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FIN_ACCT_LOCALITY',
        'target_table': 'W_FIN_ACCT_LOCALITY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.975721+00:00'
    }
) }}

WITH 

source_stsi_flex_value_set_struct AS (
    SELECT
        batch_id,
        si_flex_struct_id,
        si_flex_value_set_id,
        si_flex_value_set_desc,
        segment_name,
        db_instance,
        enabled_flag,
        related_flex_struct_id,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'stsi_flex_value_set_struct') }}
),

transformed_ex_fin_acct_locality AS (
    SELECT
    batch_id,
    bk_fin_acct_locality_int,
    financial_acct_locality_name,
    sk_si_flex_struct_id_int,
    edw_create_datetime,
    action_code,
    start_date_active,
    end_date_active,
    rank_index,
    dml_type
    FROM source_stsi_flex_value_set_struct
),

final AS (
    SELECT
        start_tv_date,
        end_tv_date,
        financial_acct_locality_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_si_flex_struct_id_int,
        bk_fin_acct_locality_int,
        action_code,
        dml_type
    FROM transformed_ex_fin_acct_locality
)

SELECT * FROM final