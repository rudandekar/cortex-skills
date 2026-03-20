{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_ra_cust_trx_ln_gldt', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_RA_CUST_TRX_LN_GLDT',
        'target_table': 'EL_OOD_RA_CUST_TRX_LN_GLDT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.573040+00:00'
    }
) }}

WITH 

source_st_ood_ra_cust_trx_ln_gldt AS (
    SELECT
        cust_trx_line_gl_dist_id,
        customer_trx_line_id,
        code_combination_id,
        line_percent,
        amount,
        gl_date,
        gl_posted_date,
        account_class,
        customer_trx_id,
        account_set_flag,
        acctd_amount,
        attribute14,
        org_id,
        attribute2,
        posting_control_id,
        creation_date,
        last_update_date,
        create_datetime,
        set_of_books_id,
        action_code
    FROM {{ source('raw', 'st_ood_ra_cust_trx_ln_gldt') }}
),

final AS (
    SELECT
        cust_trx_line_gl_dist_id,
        customer_trx_id,
        customer_trx_line_id,
        gl_date,
        gl_posted_date,
        edw_create_datetime,
        edw_update_datetime,
        code_combination_id,
        segment4,
        identifier
    FROM source_st_ood_ra_cust_trx_ln_gldt
)

SELECT * FROM final