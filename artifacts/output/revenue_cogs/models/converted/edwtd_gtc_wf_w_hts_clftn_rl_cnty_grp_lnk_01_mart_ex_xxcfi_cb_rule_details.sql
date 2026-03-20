{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_hts_clftn_rl_cnty_grp_lnk', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_HTS_CLFTN_RL_CNTY_GRP_LNK',
        'target_table': 'EX_XXCFI_CB_RULE_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.463358+00:00'
    }
) }}

WITH 

source_w_hts_clftn_rl_cnty_grp_lnk AS (
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
        end_tv_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_hts_clftn_rl_cnty_grp_lnk') }}
),

source_ex_xxcfi_cb_rule_details AS (
    SELECT
        batch_id,
        rule_details_id,
        rule_id,
        country_group_id,
        country_group_code,
        hts_code,
        start_date,
        end_date,
        active_flag,
        rule_status,
        processed_flag,
        rule_reversed,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_rule_details') }}
),

final AS (
    SELECT
        batch_id,
        rule_details_id,
        rule_id,
        country_group_id,
        country_group_code,
        hts_code,
        start_date,
        end_date,
        active_flag,
        rule_status,
        processed_flag,
        rule_reversed,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code,
        exception_type
    FROM source_ex_xxcfi_cb_rule_details
)

SELECT * FROM final