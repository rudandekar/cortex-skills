{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_orig_org_sy_plng_dst_lnk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_ORIG_ORG_SY_PLNG_DST_LNK',
        'target_table': 'N_POS_ORIG_ORG_SY_PLNG_DST_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.961848+00:00'
    }
) }}

WITH 

source_w_pos_orig_org_sy_plng_dst_lnk AS (
    SELECT
        bk_distributor_master_name,
        bk_wips_originator_id_int,
        src_rprtd_pos_originator_name,
        src_rprtd_originatr_country_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_pos_orig_org_sy_plng_dst_lnk') }}
),

final AS (
    SELECT
        bk_distributor_master_name,
        bk_wips_originator_id_int,
        src_rprtd_pos_originator_name,
        src_rprtd_originatr_country_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_pos_orig_org_sy_plng_dst_lnk
)

SELECT * FROM final