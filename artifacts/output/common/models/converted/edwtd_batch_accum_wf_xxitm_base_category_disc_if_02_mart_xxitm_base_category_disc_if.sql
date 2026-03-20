{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_xxitm_base_category_disc_if', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_XXITM_BASE_CATEGORY_DISC_IF',
        'target_table': 'XXITM_BASE_CATEGORY_DISC_IF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.077646+00:00'
    }
) }}

WITH 

source_stg_xxitm_bse_category_disc_if AS (
    SELECT
        discount_code,
        discount_name,
        base_category_number,
        base_category_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        start_date,
        end_date,
        discount,
        status,
        discount_pattern,
        cpa_code,
        discount_is_for,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_xxitm_bse_category_disc_if') }}
),

source_cg1_xxitm_bse_category_disc_if AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        discount_code,
        discount_name,
        base_category_number,
        base_category_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        start_date,
        end_date,
        discount,
        status,
        discount_pattern,
        cpa_code,
        discount_is_for
    FROM {{ source('raw', 'cg1_xxitm_bse_category_disc_if') }}
),

transformed_exp_xxitm_base_category_disc_if AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    discount_code,
    discount_name,
    base_category_number,
    base_category_name,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    start_date,
    end_date,
    discount,
    status,
    discount_pattern,
    cpa_code,
    discount_is_for
    FROM source_cg1_xxitm_bse_category_disc_if
),

final AS (
    SELECT
        discount_code,
        discount_name,
        base_category_number,
        base_category_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        start_date,
        end_date,
        discount,
        status,
        discount_pattern,
        cpa_code,
        discount_is_for,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_xxitm_base_category_disc_if
)

SELECT * FROM final