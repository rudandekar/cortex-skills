{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_lookup_values_disti', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_LOOKUP_VALUES_DISTI',
        'target_table': 'ST_CG1_FND_LOOKUP_VALUES_DISTI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.172998+00:00'
    }
) }}

WITH 

source_ff_cg1_fnd_lookup_values AS (
    SELECT
        bk_distributor_master_name,
        distributor_lvl_trnst_days_cnt,
        distributor_service_level_cd,
        distributor_region_cd,
        distributor_sub_region_cd,
        distributor_reporting_name,
        creation_date,
        created_by,
        ges_update_date,
        last_updated_by
    FROM {{ source('raw', 'ff_cg1_fnd_lookup_values') }}
),

final AS (
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
    FROM source_ff_cg1_fnd_lookup_values
)

SELECT * FROM final