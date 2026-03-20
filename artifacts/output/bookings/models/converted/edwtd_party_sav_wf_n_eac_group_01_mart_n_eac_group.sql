{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_group', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_GROUP',
        'target_table': 'N_EAC_GROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.387665+00:00'
    }
) }}

WITH 

source_w_eac_group AS (
    SELECT
        bk_eac_group_id,
        eac_group_name,
        eac_group_start_dt,
        eac_group_end_dt,
        eac_group_src_lst_updt_dtm,
        dv_eac_group_src_lst_updt_dt,
        eac_group_vertical_name,
        eac_group_sub_vertical_name,
        eac_group_sales_segment_name,
        eac_group_sub_segment_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        group_status_name,
        src_created_dtm,
        dv_src_created_dt,
        crtd_by_csco_wrkr_prty_key,
        src_lst_upd_csco_wrkr_prty_key,
        sk_eac_org_id,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_group') }}
),

final AS (
    SELECT
        bk_eac_group_id,
        eac_group_name,
        eac_group_start_dt,
        eac_group_end_dt,
        eac_group_src_lst_updt_dtm,
        dv_eac_group_src_lst_updt_dt,
        eac_group_vertical_name,
        eac_group_sub_vertical_name,
        eac_group_sales_segment_name,
        eac_group_sub_segment_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        group_status_name,
        src_created_dtm,
        dv_src_created_dt,
        crtd_by_csco_wrkr_prty_key,
        src_lst_upd_csco_wrkr_prty_key,
        sk_eac_org_id
    FROM source_w_eac_group
)

SELECT * FROM final