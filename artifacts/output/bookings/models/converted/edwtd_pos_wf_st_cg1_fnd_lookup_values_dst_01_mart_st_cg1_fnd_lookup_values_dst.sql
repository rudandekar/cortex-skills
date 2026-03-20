{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_lookup_values_dst', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_LOOKUP_VALUES_DST',
        'target_table': 'ST_CG1_FND_LOOKUP_VALUES_DST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.624266+00:00'
    }
) }}

WITH 

source_ff_cg1_fnd_lookup_values_disti1 AS (
    SELECT
        distributor_master_name,
        wips_originator_id_int,
        source_reported_pos_originator_name,
        source_reported_originator_country_code,
        created_by,
        creation_date,
        last_updated_by,
        ges_update_date
    FROM {{ source('raw', 'ff_cg1_fnd_lookup_values_disti1') }}
),

final AS (
    SELECT
        bk_distributor_master_name,
        bk_wips_originator_id_int,
        src_rprtd_pos_originator_name,
        src_rprtd_originatr_country_cd,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    FROM source_ff_cg1_fnd_lookup_values_disti1
)

SELECT * FROM final