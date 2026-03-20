{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sr_attribute', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_N_SR_ATTRIBUTE',
        'target_table': 'N_SR_ATTRIBUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.969705+00:00'
    }
) }}

WITH 

source_w_sr_attribute AS (
    SELECT
        incident_number,
        cust_theater,
        cust_country,
        code,
        creation_date,
        closed_date,
        product_id,
        node_level_3_name,
        item_name
    FROM {{ source('raw', 'w_sr_attribute') }}
),

final AS (
    SELECT
        incident_number,
        cust_theater,
        cust_country,
        code,
        creation_date,
        closed_date,
        product_id,
        node_level_3_name,
        item_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_sr_attribute
)

SELECT * FROM final