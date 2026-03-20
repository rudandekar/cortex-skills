{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_gl_je_headers', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_GL_JE_HEADERS',
        'target_table': 'EL_GL_JE_HEADERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.810032+00:00'
    }
) }}

WITH 

source_st_mf_gl_je_headers AS (
    SELECT
        batch_id,
        accrual_rev_change_sign_flag,
        accrual_rev_effective_date,
        accrual_rev_flag,
        accrual_rev_je_header_id,
        accrual_rev_period_name,
        accrual_rev_status,
        actual_flag,
        balanced_je_flag,
        balancing_segment_value,
        conversion_flag,
        created_by,
        currency_code,
        currency_conversion_date,
        currency_conversion_rate,
        currency_conversion_type,
        default_effective_date,
        description,
        encumbrance_type_id,
        from_recurring_header_id,
        ges_update_date,
        global_name,
        intercompany_mode,
        je_batch_id,
        je_category,
        je_header_id,
        je_source,
        multi_bal_seg_flag,
        name,
        originating_bal_seg_value,
        parent_je_header_id,
        period_name,
        posted_date,
        reference_date,
        reversed_je_header_id,
        set_of_books_id,
        status,
        tax_status_code,
        je_from_sla_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_gl_je_headers') }}
),

final AS (
    SELECT
        actual_flag,
        ges_update_date,
        global_name,
        je_batch_id,
        je_category,
        je_header_id,
        je_source,
        name,
        set_of_books_id,
        status,
        create_datetime,
        je_from_sla_flag
    FROM source_st_mf_gl_je_headers
)

SELECT * FROM final