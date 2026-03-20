{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_xla_distrib_link', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_XLA_DISTRIB_LINK',
        'target_table': 'PST_CLOUD_XLA_DISTRIB_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.869603+00:00'
    }
) }}

WITH 

source_cbm_xla_distribution_links AS (
    SELECT
        application_id,
        event_id,
        ae_header_id,
        ae_line_num,
        source_distribution_type,
        source_distribution_id_num_1,
        accounting_line_code,
        event_class_code,
        event_type_code,
        upg_batch_id,
        rounding_class_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_xla_distribution_links') }}
),

final AS (
    SELECT
        application_id,
        event_id,
        ae_header_id,
        ae_line_num,
        source_distribution_type,
        source_distribution_id_num_1,
        accounting_line_code,
        event_class_code,
        event_type_code,
        upg_batch_id,
        rounding_class_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_xla_distribution_links
)

SELECT * FROM final