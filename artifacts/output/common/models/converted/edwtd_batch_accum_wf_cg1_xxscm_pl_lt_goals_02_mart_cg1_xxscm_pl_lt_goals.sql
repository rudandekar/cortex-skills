{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxscm_pl_lt_goals', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXSCM_PL_LT_GOALS',
        'target_table': 'CG1_XXSCM_PL_LT_GOALS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.727351+00:00'
    }
) }}

WITH 

source_stg_cg1_xxscm_pl_lt_goals AS (
    SELECT
        product,
        organization_id,
        product_type,
        goal_lead_time,
        comments,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        ges_pk_id,
        limit_lead_time,
        web_date_range_min,
        web_date_range_max,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxscm_pl_lt_goals') }}
),

source_cg1_xxscm_pl_lt_goals AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        product,
        organization_id,
        product_type,
        goal_lead_time,
        comments,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        ges_pk_id,
        limit_lead_time,
        web_date_range_min,
        web_date_range_max
    FROM {{ source('raw', 'cg1_xxscm_pl_lt_goals') }}
),

transformed_exp_cg1_xxscm_pl_lt_goals AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    product,
    organization_id,
    product_type,
    goal_lead_time,
    comments,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    ges_pk_id,
    limit_lead_time,
    web_date_range_min,
    web_date_range_max
    FROM source_cg1_xxscm_pl_lt_goals
),

final AS (
    SELECT
        product,
        organization_id,
        product_type,
        goal_lead_time,
        comments,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        ges_pk_id,
        limit_lead_time,
        web_date_range_min,
        web_date_range_max,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxscm_pl_lt_goals
)

SELECT * FROM final