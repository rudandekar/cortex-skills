{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_supply_planning_distributor', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_SUPPLY_PLANNING_DISTRIBUTOR',
        'target_table': 'W_SUPPLY_PLANNING_DISTRIBUTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.558310+00:00'
    }
) }}

WITH 

source_st_cg1_fnd_lookup_values_disti AS (
    SELECT
        bk_distributor_master_name,
        distributor_lvl_trnst_days_cnt,
        distributor_service_level_cd,
        distributor_region_cd,
        distributor_sub_region_cd,
        distributor_reporting_name,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by
    FROM {{ source('raw', 'st_cg1_fnd_lookup_values_disti') }}
),

transformed_exptrans AS (
    SELECT
    bk_distributor_master_name,
    distributor_lvl_trnst_days_cnt,
    distributor_service_level_cd,
    distributor_region_cd,
    distributor_sub_region_cd,
    distributor_reporting_name,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_cg1_fnd_lookup_values_disti
),

final AS (
    SELECT
        bk_distributor_master_name,
        distributor_lvl_trnst_days_cnt,
        distributor_service_level_cd,
        distributor_region_cd,
        distributor_sub_region_cd,
        distributor_reporting_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final