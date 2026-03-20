{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_gl_code_combinations', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_GL_CODE_COMBINATIONS',
        'target_table': 'FF_OOD_FUSN_GL_CODE_COMBINATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.558155+00:00'
    }
) }}

WITH 

source_saas_gl_code_combinations AS (
    SELECT
        xpk_root,
        xpk_gl_code_comb,
        fk_root,
        code_combination_id,
        account_type,
        chart_of_accounts_id,
        enabled_flag,
        preserve_flag,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        start_date_active,
        end_date_active,
        last_update_date
    FROM {{ source('raw', 'saas_gl_code_combinations') }}
),

xml_parsed_xmldsq_saas_gl_code_combinations AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_gl_code_combinations src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_saas_gl_code_combinations AS (
    SELECT
    code_combination_id,
    account_type,
    chart_of_accounts_id,
    enabled_flag,
    preserve_flag,
    segment1,
    segment2,
    segment3,
    segment4,
    segment5,
    segment6,
    start_date_active,
    end_date_active,
    last_update_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_gl_code_combinations
),

final AS (
    SELECT
        code_combination_id,
        account_type,
        chart_of_accounts_id,
        enabled_flag,
        preserve_flag,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        start_date_active,
        end_date_active,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_saas_gl_code_combinations
)

SELECT * FROM final