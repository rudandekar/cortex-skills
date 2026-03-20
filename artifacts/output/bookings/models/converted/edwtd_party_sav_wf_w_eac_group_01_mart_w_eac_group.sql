{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_group', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_GROUP',
        'target_table': 'W_EAC_GROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.166177+00:00'
    }
) }}

WITH 

source_st_xxfsam_eac_group_v AS (
    SELECT
        eac_group_id,
        eac_group_name,
        eac_start_date,
        eac_end_date,
        eac_creation_date,
        eac_created_by,
        eac_last_update_date,
        eac_last_updated_by,
        eac_group_vertical_name,
        eac_group_sub_vertical_name,
        eac_group_market_segment,
        eac_group_subsegment,
        sales_motion_organization_name,
        sales_motion_organization_id,
        eac_group_status,
        batch_id
    FROM {{ source('raw', 'st_xxfsam_eac_group_v') }}
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
        sk_eac_org_id,
        action_code,
        dml_type
    FROM source_st_xxfsam_eac_group_v
)

SELECT * FROM final