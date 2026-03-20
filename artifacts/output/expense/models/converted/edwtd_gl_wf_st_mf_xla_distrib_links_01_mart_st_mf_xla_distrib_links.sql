{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_xla_distrib_links', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_XLA_DISTRIB_LINKS',
        'target_table': 'ST_MF_XLA_DISTRIB_LINKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.836800+00:00'
    }
) }}

WITH 

source_ff_mf_xla_distrib_links AS (
    SELECT
        batch_id,
        ref_ae_header_id,
        temp_line_num,
        application_id,
        ae_header_id,
        ae_line_num,
        source_distribution_type,
        source_distribution_id_num_1,
        source_distribution_id_num_2,
        upg_batch_id,
        create_datetime,
        action_code,
        global_name,
        ges_update_date,
        rounding_class_code,
        source_distribution_id_num_5
    FROM {{ source('raw', 'ff_mf_xla_distrib_links') }}
),

final AS (
    SELECT
        batch_id,
        ref_ae_header_id,
        temp_line_num,
        application_id,
        ae_header_id,
        ae_line_num,
        source_distribution_type,
        source_distribution_id_num_1,
        source_distribution_id_num_2,
        upg_batch_id,
        global_name,
        ges_update_date,
        create_datetime,
        action_code,
        rounding_class_code,
        source_distribution_id_num_5
    FROM source_ff_mf_xla_distrib_links
)

SELECT * FROM final