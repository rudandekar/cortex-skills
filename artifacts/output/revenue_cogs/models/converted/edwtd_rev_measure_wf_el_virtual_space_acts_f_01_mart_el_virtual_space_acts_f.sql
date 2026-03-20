{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_virtual_space_acts_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_VIRTUAL_SPACE_ACTS_F',
        'target_table': 'EL_VIRTUAL_SPACE_ACTS_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.659879+00:00'
    }
) }}

WITH 

source_st_virtual_space_acts AS (
    SELECT
        csone_case_number,
        case_number,
        spark_room_id,
        spark_conversation_number,
        message_content,
        created_by__c,
        created_date,
        vs_type
    FROM {{ source('raw', 'st_virtual_space_acts') }}
),

final AS (
    SELECT
        csone_case_number,
        case_number,
        spark_room_id,
        spark_conversation_number,
        message_content,
        created_by__c,
        created_date,
        vs_type,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_virtual_space_acts
)

SELECT * FROM final