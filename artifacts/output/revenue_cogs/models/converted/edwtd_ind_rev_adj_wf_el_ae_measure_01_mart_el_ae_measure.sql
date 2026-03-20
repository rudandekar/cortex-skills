{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ae_measure', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_EL_AE_MEASURE',
        'target_table': 'EL_AE_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.810961+00:00'
    }
) }}

WITH 

source_st_ae_measure AS (
    SELECT
        batch_id,
        measure_id,
        measure_name,
        measure_type_code,
        scenario,
        sub_measure_approval_flag,
        sub_measure_notification_flag,
        report_level_1,
        report_level_1_editable,
        report_level_2,
        report_level_2_editable,
        start_fiscal_period_id,
        end_fiscal_period_id,
        status_flag,
        is_rep3_submeasure_name,
        is_category_sys_generated,
        is_dept_acct_included,
        application_id,
        create_user,
        update_user,
        update_datetime,
        create_datetime,
        measure_type_flag,
        action_code,
        create_timestamp
    FROM {{ source('raw', 'st_ae_measure') }}
),

final AS (
    SELECT
        measure_id,
        measure_name,
        measure_type_code,
        create_timestamp
    FROM source_st_ae_measure
)

SELECT * FROM final