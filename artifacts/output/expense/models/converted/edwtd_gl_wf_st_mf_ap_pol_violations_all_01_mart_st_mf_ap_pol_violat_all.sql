{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_ap_pol_violations_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_AP_POL_VIOLATIONS_ALL',
        'target_table': 'ST_MF_AP_POL_VIOLAT_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.648724+00:00'
    }
) }}

WITH 

source_ff_mf_ap_pol_violations_all AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        report_header_id,
        distribution_line_number,
        violation_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        violation_type,
        allowable_amount,
        func_currency_allowable_amt,
        org_id,
        last_update_login,
        exceeded_amount,
        violation_date,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'ff_mf_ap_pol_violations_all') }}
),

final AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        report_header_id,
        distribution_line_number,
        violation_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        violation_type,
        allowable_amount,
        func_currency_allowable_amt,
        org_id,
        last_update_login,
        exceeded_amount,
        violation_date,
        ges_update_date,
        global_name
    FROM source_ff_mf_ap_pol_violations_all
)

SELECT * FROM final