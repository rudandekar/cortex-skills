{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_ar_rece_trx', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_AR_RECE_TRX',
        'target_table': 'FF_OOD_FUSN_AR_RECE_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.651501+00:00'
    }
) }}

WITH 

source_saas_ar_rece_trx AS (
    SELECT
        xpk_root,
        xpk_ar_receivables_trx,
        fk_root,
        description,
        name,
        org_id,
        receivables_trx_id,
        creation_date,
        last_update_date,
        split_key
    FROM {{ source('raw', 'saas_ar_rece_trx') }}
),

xml_parsed_xmldsq_saas_ar_rece_trx AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_ar_rece_trx src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_fusn_ar_rece_trx AS (
    SELECT
    description,
    name,
    org_id,
    receivables_trx_id,
    creation_date,
    last_update_date,
    split_key,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_ar_rece_trx
),

final AS (
    SELECT
        description,
        name,
        org_id,
        receivables_trx_id,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM transformed_exp_fusn_ar_rece_trx
)

SELECT * FROM final