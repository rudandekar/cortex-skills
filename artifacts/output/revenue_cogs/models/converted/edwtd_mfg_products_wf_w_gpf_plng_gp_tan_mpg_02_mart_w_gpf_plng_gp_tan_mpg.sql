{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gpf_plng_gp_tan_mpg', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_GPF_PLNG_GP_TAN_MPG',
        'target_table': 'W_GPF_PLNG_GP_TAN_MPG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.617505+00:00'
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
        goods_product_key,
        tan_part_key,
        inventory_orgn_name_key,
        tan_per_product_cnt,
        buyer_cisco_worker_party_key,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM transformed_exp_w_gpf_plng_gp_tan_mpg
)

SELECT * FROM final