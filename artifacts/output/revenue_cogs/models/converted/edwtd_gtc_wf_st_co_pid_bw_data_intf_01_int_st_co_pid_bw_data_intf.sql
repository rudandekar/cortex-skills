{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_co_pid_bw_data_intf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CO_PID_BW_DATA_INTF',
        'target_table': 'ST_CO_PID_BW_DATA_INTF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.496901+00:00'
    }
) }}

WITH 

source_ff_co_pid_bw_data_intf_incr AS (
    SELECT
        batch_id,
        pid_bw_data_id,
        pid,
        invalid_country,
        probabale_co,
        product_family,
        bu,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        status,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_co_pid_bw_data_intf_incr') }}
),

final AS (
    SELECT
        batch_id,
        pid_bw_data_id,
        pid,
        invalid_country,
        probabale_co,
        product_family,
        bu,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        status,
        create_datetime,
        action_code
    FROM source_ff_co_pid_bw_data_intf_incr
)

SELECT * FROM final