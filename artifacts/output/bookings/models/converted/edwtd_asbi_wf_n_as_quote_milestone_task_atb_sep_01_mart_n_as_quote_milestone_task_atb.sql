{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_quote_milestone_task_atb_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QUOTE_MILESTONE_TASK_ATB_SEP',
        'target_table': 'N_AS_QUOTE_MILESTONE_TASK_ATB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.890875+00:00'
    }
) }}

WITH 

source_w_as_quote_milestone_task_atb_sep AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        product_key,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_technology_name,
        source_deleted_flg,
        as_qt_ms_tsk_atb_split_pct_int,
        bk_as_subtechnology_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        offer_name,
        child_sku,
        deliverable_description,
        ss_cd
    FROM {{ source('raw', 'w_as_quote_milestone_task_atb_sep') }}
),

final AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        product_key,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_technology_name,
        source_deleted_flg,
        as_qt_ms_tsk_atb_split_pct_int,
        bk_as_subtechnology_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        ru_offer_name,
        ru_dv_child_sku_prdt_id
    FROM source_w_as_quote_milestone_task_atb_sep
)

SELECT * FROM final