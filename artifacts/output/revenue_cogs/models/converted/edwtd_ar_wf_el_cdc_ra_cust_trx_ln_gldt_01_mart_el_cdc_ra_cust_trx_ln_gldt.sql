{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cdc_ra_cust_trx_ln_gldt', 'realtime', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_CDC_RA_CUST_TRX_LN_GLDT',
        'target_table': 'EL_CDC_RA_CUST_TRX_LN_GLDT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.243196+00:00'
    }
) }}

WITH 

source_st_cg_ra_cust_trx_ln_gldt AS (
    SELECT
        batch_id,
        cust_trx_line_gl_dist_id,
        customer_trx_line_id,
        code_combination_id,
        set_of_books_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        line_percent,
        amount,
        gl_date,
        gl_posted_date,
        cust_trx_line_salesrep_id,
        comments,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        concatenated_segments,
        original_gl_date,
        post_request_id,
        posting_control_id,
        account_class,
        ra_post_loop_number,
        customer_trx_id,
        account_set_flag,
        acctd_amount,
        ussgl_transaction_code,
        ussgl_transaction_code_context,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        latest_rec_flag,
        org_id,
        mrc_account_class,
        mrc_customer_trx_id,
        mrc_amount,
        mrc_gl_posted_date,
        mrc_posting_control_id,
        mrc_acctd_amount,
        collected_tax_ccid,
        collected_tax_concat_seg,
        revenue_adjustment_id,
        rev_adj_class_temp,
        rec_offset_flag,
        event_id,
        user_generated_flag,
        rounding_correction_flag,
        cogs_request_id,
        ccid_change_flag,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg_ra_cust_trx_ln_gldt') }}
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
        segment4
    FROM source_st_cg_ra_cust_trx_ln_gldt
)

SELECT * FROM final