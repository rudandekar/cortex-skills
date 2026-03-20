{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fa_categories_tl_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_FA_CATEGORIES_TL_SRC2EL',
        'target_table': 'EL_FA_CATEGORIES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.738140+00:00'
    }
) }}

WITH 

source_mf_fa_categories_tl AS (
    SELECT
        category_id,
        created_by,
        creation_date,
        description,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_lang
    FROM {{ source('raw', 'mf_fa_categories_tl') }}
),

transformed_exptrans AS (
    SELECT
    category_id,
    description,
    global_name,
    language,
    source_lang,
    TO_INTEGER(CATEGORY_ID) AS out_category_id
    FROM source_mf_fa_categories_tl
),

update_strategy_upd_update AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exptrans
    WHERE DD_UPDATE != 3
),

lookup_lkptrans AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_upd_update a
    LEFT JOIN {{ source('raw', 'el_fa_categories_tl') }} b
        ON a.in_category_id = b.in_category_id
),

routed_rtr_el_fa_categories_tl AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            WHEN TRUE THEN 'INPUT'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM lookup_lkptrans
),

transformed_exp_el_fa_categories_tl AS (
    SELECT
    category_id,
    description,
    global_name,
    language,
    source_lang,
    lkp_category_id,
    lkp_global_name,
    lkp_language_code,
    lkp_description,
    lkp_source_lang
    FROM routed_rtr_el_fa_categories_tl
),

update_strategy_upd_insert AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_el_fa_categories_tl
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        category_id,
        description,
        global_name,
        language_code,
        source_lang
    FROM update_strategy_upd_insert
)

SELECT * FROM final