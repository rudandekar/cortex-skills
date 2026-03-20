{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_ra_cust_trx_ln_gldt', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_RA_CUST_TRX_LN_GLDT',
        'target_table': 'ST_OOD_FUSN_RA_CUST_TRX_LN_GLDT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.751998+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_ra_cust_trx_ln_gldt AS (
    SELECT
        account_class,
        account_set_flag,
        acctd_amount,
        amount,
        attribute14,
        attribute2,
        code_combination_id,
        cust_trx_line_gl_dist_id,
        customer_trx_id,
        customer_trx_line_id,
        gl_date,
        org_id,
        line_percent,
        posting_control_id,
        set_of_books_id,
        gl_posted_date,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_ra_cust_trx_ln_gldt') }}
),

final AS (
    SELECT
        account_class,
        account_set_flag,
        acctd_amount,
        amount,
        attribute14,
        attribute2,
        code_combination_id,
        cust_trx_line_gl_dist_id,
        customer_trx_id,
        customer_trx_line_id,
        gl_date,
        org_id,
        line_percent,
        posting_control_id,
        set_of_books_id,
        gl_posted_date,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_ra_cust_trx_ln_gldt
)

SELECT * FROM final