{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ar_distributions_all', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_AR_DISTRIBUTIONS_ALL',
        'target_table': 'EL_AR_DISTRIBUTIONS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.380113+00:00'
    }
) }}

WITH 

source_st_ar_distributions_all AS (
    SELECT
        batch_id,
        line_id,
        source_id,
        source_table,
        source_type,
        code_combination_id,
        amount_dr,
        amount_cr,
        acctd_amount_dr,
        acctd_amount_cr,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        last_update_login,
        org_id,
        source_table_secondary,
        source_id_secondary,
        currency_code,
        currency_conversion_rate,
        currency_conversion_type,
        currency_conversion_date,
        taxable_entered_dr,
        taxable_entered_cr,
        taxable_accounted_dr,
        taxable_accounted_cr,
        tax_link_id,
        third_party_id,
        third_party_sub_id,
        reversed_source_id,
        tax_code_id,
        location_segment_id,
        source_type_secondary,
        tax_group_code_id,
        ref_customer_trx_line_id,
        ref_cust_trx_line_gl_dist_id,
        ref_account_class,
        activity_bucket,
        ref_line_id,
        from_amount_dr,
        from_amount_cr,
        from_acctd_amount_dr,
        from_acctd_amount_cr,
        ref_mf_dist_flag,
        ref_dist_ccid,
        ref_prev_cust_trx_line_id,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ar_distributions_all') }}
),

final AS (
    SELECT
        line_id,
        org_id,
        source_id,
        code_combination_id,
        source_table,
        source_type,
        amount_dr,
        amount_cr,
        acctd_amount_dr,
        acctd_amount_cr,
        creation_date,
        currency_code,
        last_update_date,
        global_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_st_ar_distributions_all
)

SELECT * FROM final