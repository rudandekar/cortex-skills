{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_om_cfi_be_inv_lns_dst_al', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_OM_CFI_BE_INV_LNS_DST_AL',
        'target_table': 'EL_OM_CFI_BE_INV_LNS_DST_AL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.331744+00:00'
    }
) }}

WITH 

source_st_om_cfi_be_inv_lns_dst_al AS (
    SELECT
        batch_id,
        be_invoice_line_dist_id,
        be_invoice_line_id,
        invoice_line_percentage,
        invoice_type,
        code_combination_id,
        cogs_percentage,
        cust_trx_line_gl_dist_id,
        invoice_line_amount,
        org_id,
        global_name,
        creation_date,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_be_inv_lns_dst_al') }}
),

final AS (
    SELECT
        be_invoice_line_dist_id,
        be_invoice_line_id,
        invoice_line_percentage,
        invoice_type,
        global_name
    FROM source_st_om_cfi_be_inv_lns_dst_al
)

SELECT * FROM final