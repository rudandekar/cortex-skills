{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxicm_saf_lines_all', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXICM_SAF_LINES_ALL',
        'target_table': 'STG_CG1_XXICM_SAF_LINES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.084658+00:00'
    }
) }}

WITH 

source_cg1_xxicm_saf_lines_all AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        saf_line_id,
        saf_invoice_id,
        customer_trx_id,
        customer_trx_line_id,
        percentage,
        amount,
        tax_amount,
        quantity,
        total_amount,
        line_number,
        inventory_item_id,
        description,
        rebill_yn,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        termination_date,
        orig_po_line_ref,
        new_po_line_ref,
        correct_unit_price,
        correct_line_amount,
        start_date,
        end_date,
        invoicing_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        new_ship_to_site_use_id,
        tax_dispute_amount,
        new_freight_amount,
        new_freight_unit_price
    FROM {{ source('raw', 'cg1_xxicm_saf_lines_all') }}
),

source_stg_cg1_xxicm_saf_lines_all AS (
    SELECT
        saf_line_id,
        saf_invoice_id,
        customer_trx_id,
        customer_trx_line_id,
        percentage,
        amount,
        tax_amount,
        quantity,
        total_amount,
        line_number,
        inventory_item_id,
        description,
        rebill_yn,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        termination_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxicm_saf_lines_all') }}
),

transformed_exp_cg1_xxicm_saf_lines_all AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    saf_line_id,
    saf_invoice_id,
    customer_trx_id,
    customer_trx_line_id,
    percentage,
    amount,
    tax_amount,
    quantity,
    total_amount,
    line_number,
    inventory_item_id,
    description,
    rebill_yn,
    org_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    termination_date
    FROM source_stg_cg1_xxicm_saf_lines_all
),

final AS (
    SELECT
        saf_line_id,
        saf_invoice_id,
        customer_trx_id,
        customer_trx_line_id,
        percentage,
        amount,
        tax_amount,
        quantity,
        total_amount,
        line_number,
        inventory_item_id,
        description,
        rebill_yn,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        termination_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxicm_saf_lines_all
)

SELECT * FROM final