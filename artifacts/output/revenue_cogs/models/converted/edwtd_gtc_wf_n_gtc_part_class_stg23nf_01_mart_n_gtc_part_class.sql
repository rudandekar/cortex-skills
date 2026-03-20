{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gtc_part_class_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_GTC_PART_CLASS_STG23NF',
        'target_table': 'N_GTC_PART_CLASS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.030827+00:00'
    }
) }}

WITH 

source_n_gtc_part_class AS (
    SELECT
        gtc_part_class_cd,
        ss_cd,
        part_class_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_gtc_part_class') }}
),

final AS (
    SELECT
        gtc_part_class_cd,
        ss_cd,
        part_class_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_gtc_part_class
)

SELECT * FROM final