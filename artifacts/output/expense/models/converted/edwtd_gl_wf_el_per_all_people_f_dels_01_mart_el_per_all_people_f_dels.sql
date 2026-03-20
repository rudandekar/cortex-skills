{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_per_all_people_f_dels', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PER_ALL_PEOPLE_F_DELS',
        'target_table': 'EL_PER_ALL_PEOPLE_F_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.652006+00:00'
    }
) }}

WITH 

source_st_mf_per_all_people_f_dels AS (
    SELECT
        batch_id,
        person_id,
        effective_start_date,
        effective_end_date,
        global_name,
        ges_delete_date,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_per_all_people_f_dels') }}
),

final AS (
    SELECT
        batch_id,
        person_id,
        effective_start_date,
        effective_end_date,
        global_name,
        ges_delete_date,
        ges_update_date,
        create_datetime,
        action_code
    FROM source_st_mf_per_all_people_f_dels
)

SELECT * FROM final