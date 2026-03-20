{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_ra_batch_sources_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_RA_BATCH_SOURCES_ALL',
        'target_table': 'FF_OOD_FUSN_RA_BATCH_SOURCES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.017832+00:00'
    }
) }}

WITH 

source_saas_ra_batch_sources_all AS (
    SELECT
        xpk_root,
        xpk_ra_batch_source,
        fk_root,
        batch_source_id,
        batch_source_type,
        description,
        end_date,
        name,
        org_id,
        start_date,
        status,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'saas_ra_batch_sources_all') }}
),

xml_parsed_xmldsq_saas_ra_batch_sources_all AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_ra_batch_sources_all src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_ff_fusn_ra_batch_sources_all AS (
    SELECT
    batch_source_id,
    batch_source_type,
    description,
    end_date,
    name,
    org_id,
    start_date,
    status,
    creation_date,
    last_update_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_ra_batch_sources_all
),

final AS (
    SELECT
        batch_source_id,
        batch_source_type,
        description,
        end_date,
        name,
        org_id,
        start_date,
        status,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_ff_fusn_ra_batch_sources_all
)

SELECT * FROM final