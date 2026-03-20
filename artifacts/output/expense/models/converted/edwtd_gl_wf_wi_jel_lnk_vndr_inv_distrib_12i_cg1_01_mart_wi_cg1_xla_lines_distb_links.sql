{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_jrnl_entry_line_12i_cg1', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WI_JRNL_ENTRY_LINE_12I_CG1',
        'target_table': 'WI_CG1_XLA_LINES_DISTB_LINKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.703566+00:00'
    }
) }}

WITH 

source_el_xla_distrib_links AS (
    SELECT
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
        rounding_class_code,
        source_distribution_id_num_5
    FROM {{ source('raw', 'el_xla_distrib_links') }}
),

source_st_mf_gl_je_lines AS (
    SELECT
        batch_id,
        accounted_cr,
        accounted_dr,
        amount_includes_tax_flag,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        code_combination_id,
        created_by,
        creation_date,
        description,
        effective_date,
        entered_cr,
        entered_dr,
        gl_sl_link_id,
        gl_sl_link_table,
        invoice_amount,
        invoice_date,
        invoice_identifier,
        je_header_id,
        je_line_num,
        line_type_code,
        last_updated_by,
        last_update_date,
        no1,
        period_name,
        reference_1,
        reference_10,
        reference_2,
        reference_3,
        reference_4,
        reference_5,
        reference_6,
        reference_7,
        reference_8,
        reference_9,
        status,
        jebe_vat_alloc_flag,
        set_of_books_id,
        global_name,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_gl_je_lines') }}
),

final AS (
    SELECT
        source_distribution_id_num_1,
        global_name,
        ae_header_id,
        ae_line_num
    FROM source_st_mf_gl_je_lines
)

SELECT * FROM final