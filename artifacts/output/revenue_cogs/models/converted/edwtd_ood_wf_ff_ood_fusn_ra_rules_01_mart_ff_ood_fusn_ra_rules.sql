{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_ra_rules', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_RA_RULES',
        'target_table': 'FF_OOD_FUSN_RA_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.580902+00:00'
    }
) }}

WITH 

source_saas_ra_rules AS (
    SELECT
        xpk_root,
        xpk_ra_rules,
        fk_root,
        description,
        name,
        rule_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'saas_ra_rules') }}
),

xml_parsed_xmldsq_saas_ra_rules AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_ra_rules src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_fusn_ra_rules AS (
    SELECT
    description,
    name,
    rule_id,
    creation_date,
    last_update_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_ra_rules
),

final AS (
    SELECT
        description,
        name,
        rule_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_fusn_ra_rules
)

SELECT * FROM final