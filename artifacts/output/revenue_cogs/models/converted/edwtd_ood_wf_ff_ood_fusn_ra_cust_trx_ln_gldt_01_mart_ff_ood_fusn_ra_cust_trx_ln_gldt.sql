{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_ra_cust_trx_ln_gldt', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_RA_CUST_TRX_LN_GLDT',
        'target_table': 'FF_OOD_FUSN_RA_CUST_TRX_LN_GLDT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.168687+00:00'
    }
) }}

WITH 

source_saas_ra_cust_trx_line_gl_dist_all AS (
    SELECT
        xpk_root,
        xpk_cust_trx_line_gl_dist,
        fk_root,
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
        percent,
        posting_control_id,
        set_of_books_id,
        gl_posted_date,
        creation_date,
        last_update_date,
        split_key
    FROM {{ source('raw', 'saas_ra_cust_trx_line_gl_dist_all') }}
),

xml_parsed_xmldsq_saas_ra_cust_trx_line_gl_dist_all AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_ra_cust_trx_line_gl_dist_all src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_ra_fusn_cust_trx_line_gl_dist_all AS (
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
    percent,
    posting_control_id,
    set_of_books_id,
    gl_posted_date,
    creation_date,
    last_update_date,
    split_key,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_ra_cust_trx_line_gl_dist_all
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
    FROM transformed_exp_ra_fusn_cust_trx_line_gl_dist_all
)

SELECT * FROM final