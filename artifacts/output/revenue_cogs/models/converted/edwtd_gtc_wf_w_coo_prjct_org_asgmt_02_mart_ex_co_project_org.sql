{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_coo_prjct_org_asgmt', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_COO_PRJCT_ORG_ASGMT',
        'target_table': 'EX_CO_PROJECT_ORG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.519836+00:00'
    }
) }}

WITH 

source_ex_co_project_org AS (
    SELECT
        batch_id,
        project_org_id,
        project_id,
        org_id,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        org_type,
        org_mfr,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_co_project_org') }}
),

source_sm_coo_prjct_org_asgmt AS (
    SELECT
        coo_prjct_org_asgmt_key,
        sk_prjct_org_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_coo_prjct_org_asgmt') }}
),

source_st_co_project_org AS (
    SELECT
        batch_id,
        project_org_id,
        project_id,
        org_id,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        org_type,
        org_mfr,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_co_project_org') }}
),

source_w_coo_prjct_org_asgmt AS (
    SELECT
        coo_prjct_org_asgmt_key,
        inv_org_mfg_type,
        sk_prjct_org_id_int,
        create_dtm,
        dv_create_dt,
        last_update_dtm,
        dv_last_update_dt,
        org_type_cd,
        created_by_auto_test_flg,
        updated_by_auto_test_flg,
        bk_coo_prjct_name,
        source_deleted_flg,
        created_by_csco_wrkr_pty_key,
        last_upd_by_csco_wrkr_pty_key,
        ru_inventory_organization_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        ru_mftrng_oem_sup_pty_key,
        ru_bk_mftrng_loc_name,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_coo_prjct_org_asgmt') }}
),

final AS (
    SELECT
        batch_id,
        project_org_id,
        project_id,
        org_id,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        org_type,
        org_mfr,
        create_datetime,
        action_code,
        exception_type
    FROM source_w_coo_prjct_org_asgmt
)

SELECT * FROM final