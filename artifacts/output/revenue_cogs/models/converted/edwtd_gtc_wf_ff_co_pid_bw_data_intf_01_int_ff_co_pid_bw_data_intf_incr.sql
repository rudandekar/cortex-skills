{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_ff_co_pid_bw_data_intf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_CO_PID_BW_DATA_INTF',
        'target_table': 'FF_CO_PID_BW_DATA_INTF_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.997701+00:00'
    }
) }}

WITH 

source_co_pid_bw_data_intf AS (
    SELECT
        pid_bw_data_id,
        inventory_item_id,
        pid,
        invalid_country,
        probabale_co,
        product_family,
        bu,
        attribute1_n,
        attribute1_v,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        status
    FROM {{ source('raw', 'co_pid_bw_data_intf') }}
),

transformed_exptrans AS (
    SELECT
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
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_co_pid_bw_data_intf
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
    FROM transformed_exptrans
)

SELECT * FROM final