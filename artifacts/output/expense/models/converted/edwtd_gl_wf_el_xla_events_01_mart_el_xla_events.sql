{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xla_events', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_XLA_EVENTS',
        'target_table': 'EL_XLA_EVENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.810783+00:00'
    }
) }}

WITH 

source_st_mf_xla_events AS (
    SELECT
        event_id,
        application_id,
        event_type_code,
        event_date,
        entity_id,
        event_status_code,
        process_status_code,
        reference_num_1,
        reference_num_2,
        reference_num_3,
        reference_num_4,
        reference_char_1,
        reference_char_2,
        reference_char_3,
        reference_char_4,
        reference_date_1,
        reference_date_2,
        reference_date_3,
        reference_date_4,
        event_number,
        on_hold_flag,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        program_update_date,
        program_application_id,
        program_id,
        request_id,
        upg_batch_id,
        upg_source_application_id,
        upg_valid_flag,
        transaction_date,
        budgetary_control_flag,
        merge_event_set_id,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'st_mf_xla_events') }}
),

final AS (
    SELECT
        event_id,
        application_id,
        event_type_code,
        event_date,
        entity_id,
        event_status_code,
        process_status_code,
        event_number,
        on_hold_flag,
        creation_date,
        last_update_date,
        program_update_date,
        program_application_id,
        program_id,
        request_id,
        upg_batch_id,
        upg_source_application_id,
        upg_valid_flag,
        transaction_date,
        budgetary_control_flag,
        merge_event_set_id,
        global_name,
        ges_update_date
    FROM source_st_mf_xla_events
)

SELECT * FROM final