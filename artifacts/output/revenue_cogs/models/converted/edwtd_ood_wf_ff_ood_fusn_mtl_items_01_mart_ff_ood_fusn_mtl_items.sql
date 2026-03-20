{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_mtl_items', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_MTL_ITEMS',
        'target_table': 'FF_OOD_FUSN_MTL_ITEMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.997475+00:00'
    }
) }}

WITH 

source_saas_mtl_items AS (
    SELECT
        xpk_root,
        xpk_mtl_items,
        fk_root,
        segment1,
        segment2,
        segment3,
        creation_date,
        last_update_date,
        split_key
    FROM {{ source('raw', 'saas_mtl_items') }}
),

transformed_exp_fusn_mtl_items AS (
    SELECT
    segment1,
    segment2,
    segment3,
    creation_date,
    last_update_date,
    split_key,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_saas_mtl_items
),

xml_parsed_xmldsq_saas_mtl_items AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM transformed_exp_fusn_mtl_items src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

final AS (
    SELECT
        segment1,
        segment2,
        segment3,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM xml_parsed_xmldsq_saas_mtl_items
)

SELECT * FROM final