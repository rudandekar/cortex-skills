{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_virtual_space_acts_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_VIRTUAL_SPACE_ACTS_F',
        'target_table': 'ST_VIRTUAL_SPACE_ACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.083191+00:00'
    }
) }}

WITH 

source_ff_virtual_space_acts AS (
    SELECT
        csone_case_number,
        case_number,
        spark_room_id,
        spark_conversation_number,
        message_content,
        created_by__c,
        created_date,
        vs_type
    FROM {{ source('raw', 'ff_virtual_space_acts') }}
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
        vs_type
    FROM source_ff_virtual_space_acts
)

SELECT * FROM final