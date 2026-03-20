{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_itm_hts_clsfctn_asgnmnt_type_src23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITM_HTS_CLSFCTN_ASGNMNT_TYPE_SRC23NF',
        'target_table': 'N_ITM_HTS_CLSFCTN_ASGNMNT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.874789+00:00'
    }
) }}

WITH 

source_n_itm_hts_clsfctn_asgnmnt_type AS (
    SELECT
        bk_itm_hts_clsftn_asgmt_typ_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_itm_hts_clsfctn_asgnmnt_type') }}
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

transformed_exp_n_itm_hts_clsfctn_asgnmnt_type AS (
    SELECT
    classification_type,
    'N' AS source_deleted_flag,
    CURRENT_TIMESTAMP() AS edw_create_dtm,
    CURRENT_TIMESTAMP() AS edw_update_dtm,
    IFF(ISNULL( :LKP.LKP_N_ITM_HTS_CLSFCTN_ASGNMNT_TYPE(CLASSIFICATION_TYPE) ),'I','N') AS insert_flg
    FROM source_xxcfi_cb_item_classification
),

filtered_fltr_n_itm_hts AS (
    SELECT *
    FROM transformed_exp_n_itm_hts_clsfctn_asgnmnt_type
    WHERE INSERT_FLG='I'
),

lookup_lkp_xxfi_cb_itm_cl AS (
    SELECT
        a.*,
        b.*
    FROM filtered_fltr_n_itm_hts a
    LEFT JOIN {{ source('raw', 'xxcfi_cb_item_classification') }} b
        ON a.in_classification = b.in_classification
),

transformed_exp_n_itm_hts_update AS (
    SELECT
    bk_itm_hts_clsftn_asgmt_typ_cd,
    IFF(ISNULL( :LKP.LKP_XXFI_CB_ITM_CL(BK_ITM_HTS_CLSFTN_ASGMT_TYP_CD) ),'Y','N') AS source_deleted_flg,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM lookup_lkp_xxfi_cb_itm_cl
),

update_strategy_upd_n_itm_hts_cls AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_n_itm_hts_update
    WHERE DD_UPDATE != 3
),

filtered_fltr_n_itm_hts1 AS (
    SELECT *
    FROM update_strategy_upd_n_itm_hts_cls
    WHERE SOURCE_DELETED_FLG='Y'
),

lookup_lkp_n_itm_hts_clsfctn_asgnmnt_type AS (
    SELECT
        a.*,
        b.*
    FROM filtered_fltr_n_itm_hts1 a
    LEFT JOIN {{ source('raw', 'n_itm_hts_clsfctn_asgnmnt_type') }} b
        ON a.in_classification_type = b.in_classification_type
),

final AS (
    SELECT
        bk_itm_hts_clsftn_asgmt_typ_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM lookup_lkp_n_itm_hts_clsfctn_asgnmnt_type
)

SELECT * FROM final