{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fa_asset_invoices', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_FA_ASSET_INVOICES',
        'target_table': 'EL_FA_ASSET_INVOICES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.789492+00:00'
    }
) }}

WITH 

source_st_mf_fa_asset_invoices AS (
    SELECT
        batch_id,
        ap_distribution_line_number,
        asset_id,
        asset_invoice_id,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category_code,
        created_by,
        create_batch_date,
        create_batch_id,
        creation_date,
        date_effective,
        date_ineffective,
        deleted_flag,
        description,
        feeder_system_name,
        fixed_assets_cost,
        ges_pk_id,
        ges_update_date,
        global_name,
        invoice_date,
        invoice_id,
        invoice_number,
        invoice_transaction_id_in,
        invoice_transaction_id_out,
        last_updated_by,
        last_update_date,
        last_update_login,
        merged_code,
        merge_parent_mass_additions_id,
        parent_mass_addition_id,
        payables_batch_name,
        payables_code_combination_id,
        payables_cost,
        payables_units,
        post_batch_id,
        po_number,
        po_vendor_id,
        project_asset_line_id,
        project_id,
        source_line_id,
        split_code,
        split_merged_code,
        split_parent_mass_additions_id,
        task_id,
        unrevalued_cost,
        invoice_distribution_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fa_asset_invoices') }}
),

final AS (
    SELECT
        ap_distribution_line_number,
        asset_id,
        asset_invoice_id,
        date_effective,
        date_ineffective,
        deleted_flag,
        feeder_system_name,
        ges_pk_id,
        global_name,
        invoice_id,
        invoice_number,
        invoice_transaction_id_in,
        invoice_transaction_id_out,
        po_number,
        po_vendor_id,
        invoice_distribution_id
    FROM source_st_mf_fa_asset_invoices
)

SELECT * FROM final