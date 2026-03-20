{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_order_headers_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_ORDER_HEADERS_ALL',
        'target_table': 'EL_OOD_ORDER_HEADERS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.603693+00:00'
    }
) }}

WITH 

source_st_ood_order_headers_all AS (
    SELECT
        header_id,
        org_id,
        order_number,
        customer_acceptance_flag,
        creation_date,
        last_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_ood_order_headers_all') }}
),

transformed_exp_el_ood_order_headers_all AS (
    SELECT
    header_id,
    org_id,
    order_number,
    customer_acceptance_flag,
    creation_date,
    last_update_date,
    create_datetime,
    ss_code,
    identifier,
    DECODE(LTRIM(RTRIM(UPPER(CUSTOMER_ACCEPTANCE_FLAG))),'YES','Y','NO','N',CUSTOMER_ACCEPTANCE_FLAG) AS customer_acceptance_flag1
    FROM source_st_ood_order_headers_all
),

final AS (
    SELECT
        header_id,
        org_id,
        order_number,
        customer_acceptance_flag,
        creation_date,
        last_update_date,
        edw_update_dtm,
        ss_code,
        identifier
    FROM transformed_exp_el_ood_order_headers_all
)

SELECT * FROM final