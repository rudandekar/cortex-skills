{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gpf_plng_gp_tan_mpg', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_GPF_PLNG_GP_TAN_MPG',
        'target_table': 'EX_XXCMF_SP_PID_TAN_RLNSHP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.932667+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_pid_tan_rel_shp AS (
    SELECT
        pid,
        tan_id,
        description,
        site,
        effective_in,
        effective_out,
        quantity_per,
        supply_planner,
        mfg_planner,
        buyer_code,
        prod_family,
        business_unit,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcmf_sp_pid_tan_rel_shp') }}
),

source_st_xxcmf_sp_pid_tan_rlnshp AS (
    SELECT
        pid,
        tan_id,
        description,
        site,
        effective_in,
        effective_out,
        quantity_per,
        supply_planner,
        mfg_planner,
        buyer_code,
        prod_family,
        business_unit,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcmf_sp_pid_tan_rlnshp') }}
),

transformed_exp_w_gpf_plng_gp_tan_mpg AS (
    SELECT
    pid,
    tan_id,
    site,
    quantity_per,
    buyer_code,
    start_tv_dt,
    end_tv_dt,
    action_code,
    rank_index,
    dml_type,
    'N' AS source_deleted_flg
    FROM source_st_xxcmf_sp_pid_tan_rlnshp
),

final AS (
    SELECT
        pid,
        tan_id,
        description,
        site,
        effective_in,
        effective_out,
        quantity_per,
        supply_planner,
        mfg_planner,
        buyer_code,
        prod_family,
        business_unit,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        global_name,
        exception_type
    FROM transformed_exp_w_gpf_plng_gp_tan_mpg
)

SELECT * FROM final