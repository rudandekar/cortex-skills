{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_type_src23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_TYPE_SRC23NF',
        'target_table': 'N_FIXED_ASSET_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.723414+00:00'
    }
) }}

WITH 

source_mf_fa_lookups_tl AS (
    SELECT
        created_by,
        creation_date,
        description,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        lookup_code,
        lookup_type,
        meaning,
        source_lang
    FROM {{ source('raw', 'mf_fa_lookups_tl') }}
),

update_strategy_ins_upd_n_fixed_asset_type AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM source_mf_fa_lookups_tl
    WHERE DD_INSERT != 3
),

routed_rtr_n_fixed_asset_type AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM update_strategy_ins_upd_n_fixed_asset_type
),

update_strategy_upd_upd_n_fixed_asset_type AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtr_n_fixed_asset_type
    WHERE DD_UPDATE != 3
),

lookup_lkp_n_fixed_asset_type AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_upd_upd_n_fixed_asset_type a
    LEFT JOIN {{ source('raw', 'n_fixed_asset_type') }} b
        ON a.in_lookup_code = b.in_lookup_code
),

transformed_exp_n_fixed_asset_type AS (
    SELECT
    bk_fixed_asset_type_cd,
    fixed_asset_type_descr,
    lkp_bk_fixed_asset_type_cd,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM lookup_lkp_n_fixed_asset_type
),

final AS (
    SELECT
        bk_fixed_asset_type_cd,
        fixed_asset_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM transformed_exp_n_fixed_asset_type
)

SELECT * FROM final