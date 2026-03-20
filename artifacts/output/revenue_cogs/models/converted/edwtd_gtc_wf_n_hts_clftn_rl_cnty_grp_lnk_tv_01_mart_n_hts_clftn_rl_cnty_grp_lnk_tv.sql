{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hts_clftn_rl_cnty_grp_lnk_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HTS_CLFTN_RL_CNTY_GRP_LNK_TV',
        'target_table': 'N_HTS_CLFTN_RL_CNTY_GRP_LNK_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.934222+00:00'
    }
) }}

WITH 

source_n_hts_clftn_rl_cnty_grp_lnk_tv AS (
    SELECT
        bk_world_country_group_cd,
        hts_classification_rule_key,
        bk_hrmnzd_tariff_schedule_cd,
        bk_hts_clsfctn_rule_status_cd,
        bk_hts_effective_start_dt,
        bk_iso_country_cd,
        bk_active_flg,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm
    FROM {{ source('raw', 'n_hts_clftn_rl_cnty_grp_lnk_tv') }}
),

source_n_hts_clftn_rl_cnty_grp_lnk AS (
    SELECT
        bk_world_country_group_cd,
        hts_classification_rule_key,
        bk_hrmnzd_tariff_schedule_cd,
        bk_hts_clsfctn_rule_status_cd,
        bk_hts_effective_start_dt,
        bk_iso_country_cd,
        bk_active_flg,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_hts_clftn_rl_cnty_grp_lnk') }}
),

final AS (
    SELECT
        bk_world_country_group_cd,
        hts_classification_rule_key,
        bk_hrmnzd_tariff_schedule_cd,
        bk_hts_clsfctn_rule_status_cd,
        bk_hts_effective_start_dt,
        bk_iso_country_cd,
        bk_active_flg,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm
    FROM source_n_hts_clftn_rl_cnty_grp_lnk
)

SELECT * FROM final