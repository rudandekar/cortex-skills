{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_order_headers_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_ORDER_HEADERS_ALL',
        'target_table': 'FF_OOD_FUSN_ORDER_HEADERS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.852393+00:00'
    }
) }}

WITH 

source_saas_oe_order_headers_all AS (
    SELECT
        xpk_root,
        xpk_oe_headers,
        fk_root,
        header_id,
        org_id,
        order_number,
        customer_acceptance_flag,
        creation_date,
        last_update_date,
        split_key
    FROM {{ source('raw', 'saas_oe_order_headers_all') }}
),

xml_parsed_xmldsq_saas_oe_order_headers_all AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_oe_order_headers_all src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_ff_ood_fusn_order_headers_all AS (
    SELECT
    header_id,
    org_id,
    order_number,
    customer_acceptance_flag,
    creation_date,
    last_update_date,
    split_key,
    'I' AS action_code,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM xml_parsed_xmldsq_saas_oe_order_headers_all
),

final AS (
    SELECT
        header_id,
        org_id,
        order_number,
        customer_acceptance_flag,
        creation_date,
        last_update_date,
        split_key,
        action_code,
        create_datetime
    FROM transformed_exp_ff_ood_fusn_order_headers_all
)

SELECT * FROM final