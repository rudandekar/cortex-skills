{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_accounting_scope', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_ACCOUNTING_SCOPE',
        'target_table': 'W_ACCOUNTING_SCOPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.328108+00:00'
    }
) }}

WITH 

source_st_ngccrm_sol_ac_scope AS (
    SELECT
        batch_id,
        profile_id,
        so_header_id,
        so_line_id,
        global_name,
        org_id,
        vsoe_acct_scope,
        curr_eitf_perc,
        curr_sop_perc,
        prev_eitf_perc,
        prev_sop_perc,
        line_type,
        service_type,
        message,
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
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        last_updated_login,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ngccrm_sol_ac_scope') }}
),

final AS (
    SELECT
        bk_accounting_scope_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ngccrm_sol_ac_scope
)

SELECT * FROM final