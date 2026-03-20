{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_slc_user_report_access_stg23nf', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SLC_USER_REPORT_ACCESS_STG23NF',
        'target_table': 'N_SLC_USER_REPORT_ACCESS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.164799+00:00'
    }
) }}

WITH 

source_sc_n_slc_user_report_access AS (
    SELECT
        cec_user_id,
        strategic_logistic_center_nm,
        active_flag,
        row_id,
        line_id,
        batch_id
    FROM {{ source('raw', 'sc_n_slc_user_report_access') }}
),

transformed_exp_slc_user_upd AS (
    SELECT
    slc_user_csco_worker_party_key,
    bk_strtgc_logistics_cntr_name,
    source_deleted_flg,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM source_sc_n_slc_user_report_access
),

transformed_exp_slc_user_ins AS (
    SELECT
    slc_user_csco_worker_party_key,
    bk_strtgc_logistics_cntr_name,
    source_deleted_flg
    FROM transformed_exp_slc_user_upd
),

update_strategy_upd_slc_user AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_slc_user_ins
    WHERE DD_UPDATE != 3
),

update_strategy_ins_slc_user AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_slc_user
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        slc_user_csco_worker_party_key,
        bk_strtgc_logistics_cntr_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_ins_slc_user
)

SELECT * FROM final