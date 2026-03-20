{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_pr_cst_frst_prt_pn_p', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PR_CST_FRST_PRT_PN_P',
        'target_table': 'STG_CSF_XXCAS_PR_CST_FRST_PRT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.563234+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_pr_cst_frst_prt AS (
    SELECT
        partner_po_id,
        partner_id,
        partner_name,
        po_line_num,
        eac_cost,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        start_date,
        end_date,
        po_type,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_pr_cst_frst_prt') }}
),

source_csf_xxcas_prj_cstfrst_pt_pn_po AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        partner_po_id,
        partner_id,
        partner_name,
        po_line_num,
        eac_cost,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        start_date,
        end_date,
        po_type
    FROM {{ source('raw', 'csf_xxcas_prj_cstfrst_pt_pn_po') }}
),

transformed_exp_csf_xxcas_pr_cst_frst_prt_pn_p AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    partner_po_id,
    partner_id,
    partner_name,
    po_line_num,
    eac_cost,
    last_update_login,
    created_by,
    creation_date,
    last_update_date,
    last_updated_by,
    start_date,
    end_date,
    po_type
    FROM source_csf_xxcas_prj_cstfrst_pt_pn_po
),

final AS (
    SELECT
        partner_po_id,
        partner_id,
        partner_name,
        po_line_num,
        eac_cost,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        start_date,
        end_date,
        po_type,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_pr_cst_frst_prt_pn_p
)

SELECT * FROM final