{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_pr_lw_cst_bud_sku_mp', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PR_LW_CST_BUD_SKU_MP',
        'target_table': 'STG_CSF_XXCAS_PR_LW_CST_BUD_SKU_MP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.021703+00:00'
    }
) }}

WITH 

source_csf_xxcas_prj_lw_cst_bd_sku_mp AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        cost_bud_id_seq,
        sku_id,
        dlvrble_id,
        gpl_code,
        gpl_curr_code,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        num_of_hrs,
        hourly_rate,
        job_role,
        num_of_trips,
        trip_cost,
        num_of_night_away,
        night_cost,
        num_of_travel_hrs,
        travel_cost,
        pcogs_cost,
        hardware_cost,
        lab_fee_cost,
        rate_band
    FROM {{ source('raw', 'csf_xxcas_prj_lw_cst_bd_sku_mp') }}
),

source_stg_csf_xxcas_pr_lw_cst_bud_sku_mp AS (
    SELECT
        cost_bud_id_seq,
        sku_id,
        dlvrble_id,
        gpl_code,
        gpl_curr_code,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        num_of_hrs,
        hourly_rate,
        job_role,
        num_of_trips,
        trip_cost,
        num_of_night_away,
        night_cost,
        num_of_travel_hrs,
        travel_cost,
        pcogs_cost,
        hardware_cost,
        lab_fee_cost,
        rate_band,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_pr_lw_cst_bud_sku_mp') }}
),

transformed_exp_csf_xxcas_prj_lw_cst_bd_sku_mp AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    cost_bud_id_seq,
    sku_id,
    dlvrble_id,
    gpl_code,
    gpl_curr_code,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    num_of_hrs,
    hourly_rate,
    job_role,
    num_of_trips,
    trip_cost,
    num_of_night_away,
    night_cost,
    num_of_travel_hrs,
    travel_cost,
    pcogs_cost,
    hardware_cost,
    lab_fee_cost,
    rate_band
    FROM source_stg_csf_xxcas_pr_lw_cst_bud_sku_mp
),

final AS (
    SELECT
        cost_bud_id_seq,
        sku_id,
        dlvrble_id,
        gpl_code,
        gpl_curr_code,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        num_of_hrs,
        hourly_rate,
        job_role,
        num_of_trips,
        trip_cost,
        num_of_night_away,
        night_cost,
        num_of_travel_hrs,
        travel_cost,
        pcogs_cost,
        hardware_cost,
        lab_fee_cost,
        rate_band,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_lw_cst_bd_sku_mp
)

SELECT * FROM final