{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_pos_orig_org_sy_plng_dst_lnk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_POS_ORIG_ORG_SY_PLNG_DST_LNK',
        'target_table': 'W_POS_ORIG_ORG_SY_PLNG_DST_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.878092+00:00'
    }
) }}

WITH 

source_st_cg1_fnd_lookup_values_dst AS (
    SELECT
        bk_distributor_master_name,
        bk_wips_originator_id_int,
        src_rprtd_pos_originator_name,
        src_rprtd_originatr_country_cd,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    FROM {{ source('raw', 'st_cg1_fnd_lookup_values_dst') }}
),

transformed_exptrans AS (
    SELECT
    bk_distributor_master_name,
    bk_wips_originator_id_int,
    src_rprtd_pos_originator_name,
    src_rprtd_originatr_country_cd,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    'I' AS action_code,
    'I' AS newfield1
    FROM source_st_cg1_fnd_lookup_values_dst
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
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final