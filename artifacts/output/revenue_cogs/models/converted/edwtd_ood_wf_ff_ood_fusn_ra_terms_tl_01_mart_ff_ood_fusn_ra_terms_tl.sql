{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_ra_terms_tl', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_RA_TERMS_TL',
        'target_table': 'FF_OOD_FUSN_RA_TERMS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.211040+00:00'
    }
) }}

WITH 

source_saas_ra_terms_tl AS (
    SELECT
        xpk_root,
        xpk_ra_terms,
        fk_root,
        name,
        description,
        term_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'saas_ra_terms_tl') }}
),

xml_parsed_xmldsq_saas_ra_terms_tl AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_ra_terms_tl src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_fusn_ra_terms_tl AS (
    SELECT
    name,
    description,
    term_id,
    creation_date,
    last_update_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_ra_terms_tl
),

final AS (
    SELECT
        name,
        description,
        term_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_fusn_ra_terms_tl
)

SELECT * FROM final