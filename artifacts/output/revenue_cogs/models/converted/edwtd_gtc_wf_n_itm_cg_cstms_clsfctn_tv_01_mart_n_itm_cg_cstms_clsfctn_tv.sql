{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_itm_cg_cstms_clsfctn_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITM_CG_CSTMS_CLSFCTN_TV',
        'target_table': 'N_ITM_CG_CSTMS_CLSFCTN_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.370434+00:00'
    }
) }}

WITH 

source_n_itm_cg_cstms_clsfctn AS (
    SELECT
        item_cg_hts_clsfctn_key,
        bk_hrmnzd_tariff_schedule_cd,
        ru_bk_gtc_item_num,
        dd_item_num,
        bk_world_country_group_cd,
        bk_hts_effective_start_dt,
        item_source_type,
        hts_assigned_dt,
        item_clsfctn_modified_by_type,
        bk_itm_hts_clsftn_asgmt_typ_cd,
        hts_classification_rule_key,
        bk_iso_country_cd,
        bk_active_flg,
        ru_modified_csco_wrkr_prty_key,
        ru_mdfd_cstms_itm_clftn_sys_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_item_key,
        dv_first_occurrence_flg,
        erp_item_specific_key,
        customs_item_specific_key,
        src_created_by_dtm,
        src_crtd_by_csco_wrkr_prty_key,
        rule_modify_flg,
        dv_src_created_by_dt,
        item_clsfctn_mdfd_dtm,
        dv_item_clsfctn_mdfd_dt
    FROM {{ source('raw', 'n_itm_cg_cstms_clsfctn') }}
),

source_n_itm_cg_cstms_clsfctn_tv AS (
    SELECT
        item_cg_hts_clsfctn_key,
        bk_hrmnzd_tariff_schedule_cd,
        ru_bk_gtc_item_num,
        dd_item_num,
        bk_world_country_group_cd,
        bk_hts_effective_start_dt,
        item_source_type,
        hts_assigned_dt,
        item_clsfctn_modified_by_type,
        bk_itm_hts_clsftn_asgmt_typ_cd,
        hts_classification_rule_key,
        bk_iso_country_cd,
        bk_active_flg,
        ru_modified_csco_wrkr_prty_key,
        ru_mdfd_cstms_itm_clftn_sys_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        ru_item_key,
        dv_first_occurrence_flg,
        erp_item_specific_key,
        customs_item_specific_key,
        src_created_by_dtm,
        src_crtd_by_csco_wrkr_prty_key,
        rule_modify_flg,
        dv_src_created_by_dt,
        item_clsfctn_mdfd_dtm,
        dv_item_clsfctn_mdfd_dt
    FROM {{ source('raw', 'n_itm_cg_cstms_clsfctn_tv') }}
),

final AS (
    SELECT
        item_cg_hts_clsfctn_key,
        bk_hrmnzd_tariff_schedule_cd,
        ru_bk_gtc_item_num,
        dd_item_num,
        bk_world_country_group_cd,
        bk_hts_effective_start_dt,
        item_source_type,
        hts_assigned_dt,
        item_clsfctn_modified_by_type,
        bk_itm_hts_clsftn_asgmt_typ_cd,
        hts_classification_rule_key,
        bk_iso_country_cd,
        bk_active_flg,
        ru_modified_csco_wrkr_prty_key,
        ru_mdfd_cstms_itm_clftn_sys_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        ru_item_key,
        dv_first_occurrence_flg,
        erp_item_specific_key,
        customs_item_specific_key,
        src_created_by_dtm,
        src_crtd_by_csco_wrkr_prty_key,
        rule_modify_flg,
        dv_src_created_by_dt,
        item_clsfctn_mdfd_dtm,
        dv_item_clsfctn_mdfd_dt
    FROM source_n_itm_cg_cstms_clsfctn_tv
)

SELECT * FROM final