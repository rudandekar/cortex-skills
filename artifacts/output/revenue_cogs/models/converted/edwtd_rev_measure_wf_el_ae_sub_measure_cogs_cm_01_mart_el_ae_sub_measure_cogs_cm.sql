{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ae_sub_measure_cogs_cm', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_AE_SUB_MEASURE_COGS_CM',
        'target_table': 'EL_AE_SUB_MEASURE_COGS_CM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.789882+00:00'
    }
) }}

WITH 

source_st_ae_sub_measure_cogs_cm AS (
    SELECT
        batch_id,
        sub_measure_key,
        sub_measure_id,
        sub_measure_name,
        sub_measure_description,
        measure_id,
        source_system_id,
        old_source_system_name,
        start_effective_date,
        end_effective_date,
        start_fiscal_period_id,
        end_fiscal_period_id,
        dollar_upload_flag,
        percentage_upload_flag,
        discount_flag,
        approve_flag,
        dept_acct_flag,
        status_flag,
        source_system_adj_type_id,
        sox_tie_in,
        freq_data_posted,
        data_route,
        gmc_rollup,
        report_level_1,
        report_level_2,
        report_level_3,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ae_sub_measure_cogs_cm') }}
),

final AS (
    SELECT
        sub_measure_key,
        sub_measure_id,
        sub_measure_name,
        sub_measure_description,
        measure_id,
        source_system_id,
        old_source_system_name,
        start_effective_date,
        end_effective_date,
        start_fiscal_period_id,
        end_fiscal_period_id,
        dollar_upload_flag,
        percentage_upload_flag,
        discount_flag,
        approve_flag,
        dept_acct_flag,
        status_flag,
        source_system_adj_type_id,
        sox_tie_in,
        freq_data_posted,
        data_route,
        gmc_rollup,
        report_level_1,
        report_level_2,
        report_level_3,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM source_st_ae_sub_measure_cogs_cm
)

SELECT * FROM final