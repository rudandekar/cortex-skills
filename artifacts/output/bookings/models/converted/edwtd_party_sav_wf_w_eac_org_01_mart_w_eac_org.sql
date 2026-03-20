{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_org', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_ORG',
        'target_table': 'W_EAC_ORG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.429895+00:00'
    }
) }}

WITH 

source_st_xxfsam_org_v AS (
    SELECT
        organization_id,
        organization_name,
        start_date,
        end_date,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by
    FROM {{ source('raw', 'st_xxfsam_org_v') }}
),

final AS (
    SELECT
        bk_eac_org_id,
        eac_org_name,
        org_start_dt,
        org_end_dt,
        src_created_dtm,
        dv_src_created_dt,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        src_lst_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxfsam_org_v
)

SELECT * FROM final