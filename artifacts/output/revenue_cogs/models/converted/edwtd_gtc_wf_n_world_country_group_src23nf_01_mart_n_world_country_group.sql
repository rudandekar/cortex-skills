{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_world_country_group_src23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_WORLD_COUNTRY_GROUP_SRC23NF',
        'target_table': 'N_WORLD_COUNTRY_GROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.646194+00:00'
    }
) }}

WITH 

source_n_world_country_group AS (
    SELECT
        bk_world_country_group_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_world_country_group') }}
),

source_xxcfi_cb_item_classification AS (
    SELECT
        item_classification_id,
        item_id,
        item_name,
        inventory_item_id,
        item_type,
        erp_specific,
        custom_specific,
        hts_code,
        start_date,
        end_date,
        country_group_id,
        country_group_code,
        rule_id,
        classification_type,
        tc_interface_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        end_dated_flag,
        nextlinx_rule_id,
        hts_start_date,
        item_flag
    FROM {{ source('raw', 'xxcfi_cb_item_classification') }}
),

filtered_fltr_n_world_country_code1 AS (
    SELECT *
    FROM source_xxcfi_cb_item_classification
    WHERE SRC_DELETED_FLAG='Y'
),

lookup_lkp_n_world_country_group AS (
    SELECT
        a.*,
        b.*
    FROM filtered_fltr_n_world_country_code1 a
    LEFT JOIN {{ source('raw', 'n_world_country_group') }} b
        ON a.in_country_group_code = b.in_country_group_code
),

transformed_exp_n_world_country_group AS (
    SELECT
    country_group_code,
    'N' AS source_deleted_flag,
    CURRENT_TIMESTAMP() AS edw_create_dtm,
    CURRENT_TIMESTAMP() AS edw_update_dtm,
    IFF(ISNULL(:LKP.LKP_N_WORLD_COUNTRY_GROUP(COUNTRY_GROUP_CODE)),'I','N') AS insert_flg
    FROM lookup_lkp_n_world_country_group
),

filtered_fltr_n_world_country_code AS (
    SELECT *
    FROM transformed_exp_n_world_country_group
    WHERE INSERT_FLG='I'
),

transformed_exp_n_world_country_group_update AS (
    SELECT
    bk_world_country_group_cd,
    CURRENT_TIMESTAMP() AS edw_update_dtm,
    IFF(ISNULL( :LKP.LKP_XXCFI_CB_ITEM_CLASSIFICATION(BK_WORLD_COUNTRY_GROUP_CD)),'Y','N') AS src_deleted_flag
    FROM filtered_fltr_n_world_country_code
),

update_strategy_upd_n_world_country_code AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_n_world_country_group_update
    WHERE DD_UPDATE != 3
),

lookup_lkp_xxcfi_cb_item_classification AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_upd_n_world_country_code a
    LEFT JOIN {{ source('raw', 'xxcfi_cb_item_classification') }} b
        ON a.in_country_group_code = b.in_country_group_code
),

final AS (
    SELECT
        bk_world_country_group_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM lookup_lkp_xxcfi_cb_item_classification
)

SELECT * FROM final