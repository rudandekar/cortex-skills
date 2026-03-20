{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_supply_planning_distributor', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_SUPPLY_PLANNING_DISTRIBUTOR',
        'target_table': 'N_SUPPLY_PLANNING_DISTRIBUTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.375677+00:00'
    }
) }}

WITH 

source_w_supply_planning_distributor AS (
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
    FROM {{ source('raw', 'w_supply_planning_distributor') }}
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
        edw_update_user
    FROM source_w_supply_planning_distributor
)

SELECT * FROM final