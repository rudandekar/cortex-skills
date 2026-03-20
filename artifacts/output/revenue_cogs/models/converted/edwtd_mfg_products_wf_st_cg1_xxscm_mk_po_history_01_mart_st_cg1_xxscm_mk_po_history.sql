{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxscm_mk_po_history', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXSCM_MK_PO_HISTORY',
        'target_table': 'ST_CG1_XXSCM_MK_PO_HISTORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.323698+00:00'
    }
) }}

WITH 

source_cg1_xxscm_mk_po_history AS (
    SELECT
        message_3b2_id,
        po_number,
        asn_process_date,
        message_bom_pack_id,
        bom_ref_id,
        bom_parent_ref_id,
        item_number,
        item_revision,
        item_type,
        uom,
        bom_item_quantity,
        org_code,
        flb_cost,
        df_fee_cost,
        currency_code,
        planning_code,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        cost_updated_flag,
        source_dml_type,
        source_commit_time,
        trail_file_name,
        refresh_datetime
    FROM {{ source('raw', 'cg1_xxscm_mk_po_history') }}
),

final AS (
    SELECT
        message_3b2_id,
        po_number,
        asn_process_date,
        message_bom_pack_id,
        bom_ref_id,
        bom_parent_ref_id,
        item_number,
        item_revision,
        item_type,
        uom,
        bom_item_quantity,
        org_code,
        flb_cost,
        df_fee_cost,
        currency_code,
        planning_code,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        cost_updated_flag,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        confis_tax_amount,
        icms_tax_amount,
        rnd_tax_amount,
        pis_tax_amount
    FROM source_cg1_xxscm_mk_po_history
)

SELECT * FROM final