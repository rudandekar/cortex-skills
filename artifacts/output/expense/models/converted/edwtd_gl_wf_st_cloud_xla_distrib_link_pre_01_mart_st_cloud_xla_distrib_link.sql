{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cloud_xla_distrib_link_pre', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CLOUD_XLA_DISTRIB_LINK_PRE',
        'target_table': 'ST_CLOUD_XLA_DISTRIB_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.904170+00:00'
    }
) }}

WITH 

source_pst_cloud_xla_distrib_link AS (
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
    FROM {{ source('raw', 'pst_cloud_xla_distrib_link') }}
),

final AS (
    SELECT
        application_id,
        event_id,
        ae_header_id,
        ae_line_num,
        source_distribution_type,
        ref_ae_header_id,
        merge_duplicate_code,
        temp_line_num,
        source_distribution_id_char_1,
        source_distribution_id_char_2,
        source_distribution_id_char_3,
        source_distribution_id_char_4,
        source_distribution_id_char_5,
        source_distribution_id_num_1,
        source_distribution_id_num_2,
        source_distribution_id_num_3,
        source_distribution_id_num_4,
        source_distribution_id_num_5,
        tax_line_ref_id,
        tax_summary_line_ref_id,
        tax_rec_nrec_dist_ref_id,
        statistical_amount,
        ref_temp_line_num,
        accounting_line_code,
        accounting_line_type_code,
        ref_event_id,
        line_definition_owner_code,
        line_definition_code,
        event_class_code,
        event_type_code,
        upg_batch_id,
        calculate_acctd_amts_flag,
        calculate_g_l_amts_flag,
        rounding_class_code,
        document_rounding_level,
        unrounded_entered_dr,
        unrounded_entered_cr,
        doc_rounding_entered_amt,
        doc_rounding_acctd_amt,
        unrounded_accounted_cr,
        unrounded_accounted_dr,
        ges_update_date,
        global_name,
        create_datetime,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_xla_distrib_link
)

SELECT * FROM final