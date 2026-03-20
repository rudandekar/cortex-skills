{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_as_rev_to_gl_rae_link', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_AS_REV_TO_GL_RAE_LINK',
        'target_table': 'WI_AST_REVENUE_JE_DESCR_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.406644+00:00'
    }
) }}

WITH 

source_mt_ast_revenue_je_descr_link AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_fiscal_year_mth_num_int,
        journal_entry_line_descr,
        dv_event_id,
        dv_bk_as_project_cd,
        dv_milestone_id,
        dv_created_by_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user
    FROM {{ source('raw', 'mt_ast_revenue_je_descr_link') }}
),

source_wi_ast_revenue_event_link AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        journal_entry_line_descr,
        transaction_type_category_cd
    FROM {{ source('raw', 'wi_ast_revenue_event_link') }}
),

source_wi_ast_revenue_je_descr_link AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_fiscal_year_mth_num_int,
        journal_entry_line_descr,
        dv_event_id,
        project_milestone_user,
        dv_bk_as_project_cd,
        milestone_user,
        dv_milestone_id,
        dv_created_by_user
    FROM {{ source('raw', 'wi_ast_revenue_je_descr_link') }}
),

final AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_fiscal_year_mth_num_int,
        journal_entry_line_descr,
        dv_event_id,
        project_milestone_user,
        dv_bk_as_project_cd,
        milestone_user,
        dv_milestone_id,
        dv_created_by_user,
        bk_journal_entry_number_int,
        bk_journal_entry_line_num_int,
        rae_adjustment_gl_dstrbtn_key
    FROM source_wi_ast_revenue_je_descr_link
)

SELECT * FROM final