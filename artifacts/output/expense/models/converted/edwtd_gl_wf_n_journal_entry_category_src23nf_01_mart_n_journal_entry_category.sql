{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_journal_entry_category_src23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_JOURNAL_ENTRY_CATEGORY_SRC23NF',
        'target_table': 'N_JOURNAL_ENTRY_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.153317+00:00'
    }
) }}

WITH 

source_mf_gl_je_categories_tl AS (
    SELECT
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        consolidation_flag,
        context,
        created_by,
        creation_date,
        description,
        ges_update_date,
        global_name,
        je_category_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        reversal_option_code,
        source_lang,
        user_je_category_name
    FROM {{ source('raw', 'mf_gl_je_categories_tl') }}
),

lookup_lkp_n_source_system_codes AS (
    SELECT
        a.*,
        b.*
    FROM source_mf_gl_je_categories_tl a
    LEFT JOIN {{ source('raw', 'n_source_system_codes') }} b
        ON a.in_global_name = b.in_global_name
),

routed_rtr_n_journal_entry_category AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM lookup_lkp_n_source_system_codes
),

update_strategy_upd_upd_n_journal_entry_category AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtr_n_journal_entry_category
    WHERE DD_UPDATE != 3
),

update_strategy_ins_upd_n_journal_entry_category AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_upd_n_journal_entry_category
    WHERE DD_INSERT != 3
),

transformed_exp_n_journal_entry_category AS (
    SELECT
    lkp_bk_journal_entry_category_name,
    lkp_ss_cd,
    bk_journal_entry_category_name,
    journal_entry_category_descr,
    source_system_code,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM update_strategy_ins_upd_n_journal_entry_category
),

lookup_lkp_n_journal_entry_category AS (
    SELECT
        a.*,
        b.*
    FROM transformed_exp_n_journal_entry_category a
    LEFT JOIN {{ source('raw', 'n_journal_entry_category') }} b
        ON a.in_je_category_name = b.in_je_category_name
),

final AS (
    SELECT
        bk_journal_entry_category_name,
        journal_entry_category_descr,
        ss_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM lookup_lkp_n_journal_entry_category
)

SELECT * FROM final