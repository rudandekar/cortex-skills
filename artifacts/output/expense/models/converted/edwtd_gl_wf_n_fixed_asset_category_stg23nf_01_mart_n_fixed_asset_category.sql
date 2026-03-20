{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_category_stg23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_CATEGORY_STG23NF',
        'target_table': 'N_FIXED_ASSET_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.122174+00:00'
    }
) }}

WITH 

source_st_mf_fa_categories_b AS (
    SELECT
        batch_id,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category_code,
        capitalize_flag,
        category_id,
        category_type,
        created_by,
        creation_date,
        date_ineffective,
        enabled_flag,
        end_date_active,
        ges_update_date,
        global_attribute1,
        global_attribute10,
        global_attribute11,
        global_attribute12,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute16,
        global_attribute17,
        global_attribute18,
        global_attribute19,
        global_attribute2,
        global_attribute20,
        global_attribute3,
        global_attribute4,
        global_attribute5,
        global_attribute6,
        global_attribute7,
        global_attribute8,
        global_attribute9,
        global_attribute_category,
        global_name,
        inventorial,
        last_updated_by,
        last_update_date,
        last_update_login,
        owned_leased,
        production_capacity,
        property_1245_1250_code,
        property_type_code,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        segment7,
        start_date_active,
        summary_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fa_categories_b') }}
),

transformed_exp_n_fixed_asset_category_upd AS (
    SELECT
    bk_asset_category_cd,
    bk_asset_subcategory_cd,
    asset_category_name,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM source_st_mf_fa_categories_b
),

update_strategy_upd_upd_n_fixed_asset_category AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_n_fixed_asset_category_upd
    WHERE DD_UPDATE != 3
),

transformed_exp_n_fixed_asset_category_ins AS (
    SELECT
    bk_asset_category_cd,
    bk_asset_subcategory_cd,
    asset_category_name
    FROM update_strategy_upd_upd_n_fixed_asset_category
),

update_strategy_upd_ins_n_fixed_asset_category AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_n_fixed_asset_category_ins
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        bk_asset_category_cd,
        bk_asset_subcategory_cd,
        asset_category_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_ins_n_fixed_asset_category
)

SELECT * FROM final