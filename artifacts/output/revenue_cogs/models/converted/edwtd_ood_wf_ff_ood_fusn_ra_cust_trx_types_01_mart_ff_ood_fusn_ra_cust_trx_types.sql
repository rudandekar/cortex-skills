{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_ra_cust_trx_types', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_RA_CUST_TRX_TYPES',
        'target_table': 'FF_OOD_FUSN_RA_CUST_TRX_TYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.180408+00:00'
    }
) }}

WITH 

source_saas_ra_cust_trx_types_all AS (
    SELECT
        xpk_root,
        xpk_ra_cust_trx_type,
        fk_root,
        cust_trx_type_id,
        description,
        name,
        org_id,
        type,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'saas_ra_cust_trx_types_all') }}
),

xml_parsed_xmldsq_saas_ra_cust_trx_types_all AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_ra_cust_trx_types_all src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_fusn_ra_cust_trx_types_all AS (
    SELECT
    cust_trx_type_id,
    description,
    name,
    org_id,
    type,
    creation_date,
    last_update_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_ra_cust_trx_types_all
),

final AS (
    SELECT
        cust_trx_type_id,
        description,
        name,
        org_id,
        type_code,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_fusn_ra_cust_trx_types_all
)

SELECT * FROM final