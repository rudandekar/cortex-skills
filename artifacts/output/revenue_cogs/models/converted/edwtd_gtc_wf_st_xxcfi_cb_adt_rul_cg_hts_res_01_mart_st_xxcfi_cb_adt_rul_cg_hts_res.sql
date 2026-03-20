{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_adt_rul_cg_hts_res', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_ADT_RUL_CG_HTS_RES',
        'target_table': 'ST_XXCFI_CB_ADT_RUL_CG_HTS_RES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.673014+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_adt_rul_cg_hts_res AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_month,
        audit_year,
        audit_fiscal_year,
        rule_id,
        specific_name,
        pid_count,
        country_group_code,
        hts_code,
        duty_rate,
        score,
        rule_cg_comment,
        pool_target_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_hts_code,
        audit_performed,
        specific_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_adt_rul_cg_hts_res') }}
),

final AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_month,
        audit_year,
        audit_fiscal_year,
        rule_id,
        specific_name,
        pid_count,
        country_group_code,
        hts_code,
        duty_rate,
        score,
        rule_cg_comment,
        pool_target_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_hts_code,
        audit_performed,
        specific_id,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_adt_rul_cg_hts_res
)

SELECT * FROM final