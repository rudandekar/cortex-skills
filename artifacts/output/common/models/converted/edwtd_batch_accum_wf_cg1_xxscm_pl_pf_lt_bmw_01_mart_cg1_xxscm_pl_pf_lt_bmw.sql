{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxscm_pl_pf_lt_bmw', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXSCM_PL_PF_LT_BMW',
        'target_table': 'CG1_XXSCM_PL_PF_LT_BMW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.706652+00:00'
    }
) }}

WITH 

source_stg_cg1_xxscm_pl_pf_lt_bmw AS (
    SELECT
        product_family,
        last_updated_by,
        last_update_date,
        goal_lead_time,
        creation_date,
        created_by,
        attribute9,
        attribute8,
        attribute7,
        attribute6,
        attribute5,
        attribute4,
        attribute3,
        attribute2,
        attribute15,
        attribute14,
        attribute13,
        attribute12,
        attribute11,
        attribute10,
        attribute1,
        attribute_category,
        source_commit_time,
        trail_file_name,
        source_dml_type,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxscm_pl_pf_lt_bmw') }}
),

source_cg1_xxscm_pl_pf_lt_bmw AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        product_family,
        goal_lead_time,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15
    FROM {{ source('raw', 'cg1_xxscm_pl_pf_lt_bmw') }}
),

transformed_exp_cg1_xxscm_pl_pf_lt_bmw AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    product_family,
    goal_lead_time,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15
    FROM source_cg1_xxscm_pl_pf_lt_bmw
),

final AS (
    SELECT
        product_family,
        last_updated_by,
        last_update_date,
        goal_lead_time,
        creation_date,
        created_by,
        attribute9,
        attribute8,
        attribute7,
        attribute6,
        attribute5,
        attribute4,
        attribute3,
        attribute2,
        attribute15,
        attribute14,
        attribute13,
        attribute12,
        attribute11,
        attribute10,
        attribute1,
        attribute_category,
        source_commit_time,
        trail_file_name,
        source_dml_type,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxscm_pl_pf_lt_bmw
)

SELECT * FROM final