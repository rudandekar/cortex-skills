{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfir_revenue_dist_v', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFIR_REVENUE_DIST_V',
        'target_table': 'EL_XXCFIR_REVENUE_DIST_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.154979+00:00'
    }
) }}

WITH 

source_el_xxcfir_revenue_dist_v AS (
    SELECT
        transaction_dist_id,
        application_id,
        ledger_id,
        org_id,
        ae_header_id,
        ae_line_num,
        transaction_id,
        trx_line_id,
        amount,
        gl_date,
        currency_code,
        account_class,
        header_id,
        order_line_id,
        ccid,
        dist_bal_type,
        je_category_name,
        je_source_name,
        gl_posted_date,
        attribute2,
        accounted_amount,
        parent_sub_sku_id,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM {{ source('raw', 'el_xxcfir_revenue_dist_v') }}
),

final AS (
    SELECT
        transaction_dist_id,
        application_id,
        ledger_id,
        org_id,
        ae_header_id,
        ae_line_num,
        transaction_id,
        trx_line_id,
        amount,
        gl_date,
        currency_code,
        account_class,
        header_id,
        order_line_id,
        ccid,
        dist_bal_type,
        je_category_name,
        je_source_name,
        gl_posted_date,
        attribute2,
        accounted_amount,
        parent_sub_sku_id,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_el_xxcfir_revenue_dist_v
)

SELECT * FROM final