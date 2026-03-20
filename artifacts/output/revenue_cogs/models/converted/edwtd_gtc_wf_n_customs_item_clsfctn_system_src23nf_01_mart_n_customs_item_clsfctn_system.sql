{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_customs_item_clsfctn_system_src23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOMS_ITEM_CLSFCTN_SYSTEM_SRC23NF',
        'target_table': 'N_CUSTOMS_ITEM_CLSFCTN_SYSTEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.381450+00:00'
    }
) }}

WITH 

source_n_customs_item_clsfctn_system AS (
    SELECT
        bk_customs_item_clsfctn_sys_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_customs_item_clsfctn_system') }}
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

filtered_fltr_n_cutoms_upd AS (
    SELECT *
    FROM source_xxcfi_cb_item_classification
    WHERE SOURCE_DELETED_FLG='Y'
),

lookup_lkp_n_customs_item_clsfctn_system AS (
    SELECT
        a.*,
        b.*
    FROM filtered_fltr_n_cutoms_upd a
    LEFT JOIN {{ source('raw', 'n_customs_item_clsfctn_system') }} b
        ON a.in_modified = b.in_modified
),

transformed_exp_n_customs_item_clsfctn_system AS (
    SELECT
    modified_by,
    'N' AS sourec_deleted_flg,
    IFF(ISNULL(:LKP.LKP_N_CUSTOMS_ITEM_CLSFCTN_SYSTEM(MODIFIED_BY)),'I','N') AS insert_flg,
    CURRENT_TIMESTAMP() AS edw_create_dtm,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM lookup_lkp_n_customs_item_clsfctn_system
),

filtered_fltr_n_cutoms AS (
    SELECT *
    FROM transformed_exp_n_customs_item_clsfctn_system
    WHERE INSERT_FLG='I'
),

lookup_lkp_xxcfi_cb_item_classification AS (
    SELECT
        a.*,
        b.*
    FROM filtered_fltr_n_cutoms a
    LEFT JOIN {{ source('raw', 'xxcfi_cb_item_classification') }} b
        ON a.in_modified = b.in_modified
),

transformed_exp_n_customs_item_clsfctn_system1 AS (
    SELECT
    bk_customs_item_clsfctn_sys_cd,
    IFF(ISNULL(:LKP.LKP_XXCFI_CB_ITEM_CLASSIFICATION(BK_CUSTOMS_ITEM_CLSFCTN_SYS_CD)),'Y','N') AS source_deleted_flg,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM lookup_lkp_xxcfi_cb_item_classification
),

update_strategy_upd_n_customs_item_clsfctn_system AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_n_customs_item_clsfctn_system1
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        bk_customs_item_clsfctn_sys_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_n_customs_item_clsfctn_system
)

SELECT * FROM final