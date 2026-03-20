{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hts_clsfctn_rule_status_src23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HTS_CLSFCTN_RULE_STATUS_SRC23NF',
        'target_table': 'N_HTS_CLSFCTN_RULE_STATUS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.926874+00:00'
    }
) }}

WITH 

source_n_hts_clsfctn_rule_status AS (
    SELECT
        bk_hts_clsfctn_rule_status_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_hts_clsfctn_rule_status') }}
),

source_xxcfi_cb_rule_details AS (
    SELECT
        rule_details_id,
        rule_id,
        country_group_id,
        country_group_code,
        hts_code,
        start_date,
        end_date,
        active_flag,
        rule_status,
        processed_flag,
        rule_reversed,
        created_by,
        created_date,
        modified_by,
        modified_date
    FROM {{ source('raw', 'xxcfi_cb_rule_details') }}
),

filtered_fltr_n_hts_clsfctn_rule_status1 AS (
    SELECT *
    FROM source_xxcfi_cb_rule_details
    WHERE SOURCE_DELETED_FLG='Y'
),

transformed_exp_n_hts_clsfctn_rule_status AS (
    SELECT
    rule_status,
    'N' AS source_deleted_flag,
    CURRENT_TIMESTAMP() AS edw_create_dtm,
    CURRENT_TIMESTAMP() AS edw_update_dtm,
    IFF(ISNULL( :LKP.LKP_N_HTS_CLSFCTN_RULE_STATUS(RULE_STATUS)),'I','N') AS insert_flg
    FROM filtered_fltr_n_hts_clsfctn_rule_status1
),

lookup_lkp_n_hts_clsfctn_rule_status AS (
    SELECT
        a.*,
        b.*
    FROM transformed_exp_n_hts_clsfctn_rule_status a
    LEFT JOIN {{ source('raw', 'n_hts_clsfctn_rule_status') }} b
        ON a.in_rule_status = b.in_rule_status
),

filtered_fltr_n_hts_clsfctn_rule_status AS (
    SELECT *
    FROM lookup_lkp_n_hts_clsfctn_rule_status
    WHERE INSERT_FLG='I'
),

update_strategy_upd_n_hts_clsfctn_rule_status AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM filtered_fltr_n_hts_clsfctn_rule_status
    WHERE DD_UPDATE != 3
),

transformed_exp_n_hts_clsfctn_rule_status_update AS (
    SELECT
    bk_hts_clsfctn_rule_status_cd,
    IFF(ISNULL( :LKP.LKP_XXCFI_CB_RULE_DETAILS(BK_HTS_CLSFCTN_RULE_STATUS_CD)),'Y','N') AS source_deleted_flg,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM update_strategy_upd_n_hts_clsfctn_rule_status
),

lookup_lkp_xxcfi_cb_rule_details AS (
    SELECT
        a.*,
        b.*
    FROM transformed_exp_n_hts_clsfctn_rule_status_update a
    LEFT JOIN {{ source('raw', 'xxcfi_cb_rule_details') }} b
        ON a.in_rule_status = b.in_rule_status
),

final AS (
    SELECT
        bk_hts_clsfctn_rule_status_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM lookup_lkp_xxcfi_cb_rule_details
)

SELECT * FROM final