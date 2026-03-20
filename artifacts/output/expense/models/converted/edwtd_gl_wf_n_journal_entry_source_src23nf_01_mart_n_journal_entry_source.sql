{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_journal_entry_source_src23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_JOURNAL_ENTRY_SOURCE_SRC23NF',
        'target_table': 'N_JOURNAL_ENTRY_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.884469+00:00'
    }
) }}

WITH 

source_st_mf_gl_je_sources_tl AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        effective_date_rule_code,
        ges_update_date,
        global_name,
        je_source_name,
        journal_approval_flag,
        journal_reference_flag,
        language_code,
        last_updated_by,
        last_update_date,
        last_update_login,
        override_edits_flag,
        source_lang,
        user_je_source_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_gl_je_sources_tl') }}
),

lookup_lkp_n_journal_entry_source AS (
    SELECT
        a.*,
        b.*
    FROM source_st_mf_gl_je_sources_tl a
    LEFT JOIN {{ source('raw', 'n_journal_entry_source') }} b
        ON a.in_je_source_name = b.in_je_source_name
),

lookup_lkp_n_source_system_codes AS (
    SELECT
        a.*,
        b.*
    FROM lookup_lkp_n_journal_entry_source a
    LEFT JOIN {{ source('raw', 'n_source_system_codes') }} b
        ON a.in_global_name = b.in_global_name
),

transformed_exp_n_journal_entry_source AS (
    SELECT
    lkp_bk_journal_entry_source_name,
    lkp_ss_cd,
    bk_journal_entry_source_name,
    journal_entry_source_descr,
    user_defined_name,
    source_system_code,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM lookup_lkp_n_source_system_codes
),

routed_rtr_n_journal_entry_source AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exp_n_journal_entry_source
),

update_strategy_ins_n_journal_entry_source AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtr_n_journal_entry_source
    WHERE DD_INSERT != 3
),

update_strategy_upd_n_journal_entry_source AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_ins_n_journal_entry_source
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        bk_journal_entry_source_name,
        journal_entry_source_descr,
        user_defined_name,
        ss_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM update_strategy_upd_n_journal_entry_source
)

SELECT * FROM final