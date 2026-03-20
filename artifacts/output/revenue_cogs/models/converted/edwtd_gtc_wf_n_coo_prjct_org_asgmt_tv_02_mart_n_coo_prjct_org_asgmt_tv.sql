{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_coo_prjct_org_asgmt_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_COO_PRJCT_ORG_ASGMT_TV',
        'target_table': 'N_COO_PRJCT_ORG_ASGMT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.304173+00:00'
    }
) }}

WITH 

source_n_coo_prjct_org_asgmt AS (
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
        ru_mftrng_oem_sup_pty_key,
        ru_bk_mftrng_loc_name
    FROM {{ source('raw', 'n_coo_prjct_org_asgmt') }}
),

source_n_coo_prjct_org_asgmt_tv AS (
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
        ru_bk_mftrng_loc_name
    FROM {{ source('raw', 'n_coo_prjct_org_asgmt_tv') }}
),

final AS (
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
        ru_bk_mftrng_loc_name
    FROM source_n_coo_prjct_org_asgmt_tv
)

SELECT * FROM final