{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_hz_timezones', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_HZ_TIMEZONES',
        'target_table': 'CSF_HZ_TIMEZONES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.809691+00:00'
    }
) }}

WITH 

source_stg_hz_timezones AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        timezone_id,
        global_timezone_name,
        gmt_deviation_hours,
        daylight_savings_time_flag,
        begin_dst_month,
        begin_dst_day,
        begin_dst_week_of_month,
        begin_dst_day_of_week,
        begin_dst_hour,
        end_dst_month,
        end_dst_day,
        end_dst_week_of_month,
        end_dst_day_of_week,
        end_dst_hour,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        standard_time_short_code,
        daylight_savings_short_code,
        primary_zone_flag
    FROM {{ source('raw', 'stg_hz_timezones') }}
),

source_hz_timezones AS (
    SELECT
        ges_update_date,
        timezone_id,
        global_timezone_name,
        gmt_deviation_hours,
        daylight_savings_time_flag,
        begin_dst_month,
        begin_dst_day,
        begin_dst_week_of_month,
        begin_dst_day_of_week,
        begin_dst_hour,
        end_dst_month,
        end_dst_day,
        end_dst_week_of_month,
        end_dst_day_of_week,
        end_dst_hour,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        standard_time_short_code,
        daylight_savings_short_code,
        primary_zone_flag
    FROM {{ source('raw', 'hz_timezones') }}
),

transformed_exptrans AS (
    SELECT
    ges_update_date,
    timezone_id,
    global_timezone_name,
    gmt_deviation_hours,
    daylight_savings_time_flag,
    begin_dst_month,
    begin_dst_day,
    begin_dst_week_of_month,
    begin_dst_day_of_week,
    begin_dst_hour,
    end_dst_month,
    end_dst_day,
    end_dst_week_of_month,
    end_dst_day_of_week,
    end_dst_hour,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    standard_time_short_code,
    daylight_savings_short_code,
    primary_zone_flag,
    source_commit_time,
    'INSERT' AS source_dml_type
    FROM source_hz_timezones
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        timezone_id,
        global_timezone_name,
        gmt_deviation_hours,
        daylight_savings_time_flag,
        begin_dst_month,
        begin_dst_day,
        begin_dst_week_of_month,
        begin_dst_day_of_week,
        begin_dst_hour,
        end_dst_month,
        end_dst_day,
        end_dst_week_of_month,
        end_dst_day_of_week,
        end_dst_hour,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        standard_time_short_code,
        daylight_savings_short_code,
        primary_zone_flag
    FROM transformed_exptrans
)

SELECT * FROM final