{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_quote_milestone_task_atb_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QUOTE_MILESTONE_TASK_ATB_HDP',
        'target_table': 'ST_AS_QUOTE_MILESTONE_TASK_ATB_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.516848+00:00'
    }
) }}

WITH 

source_ff_as_quote_milestone_task_atb_hdp AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        product_key,
        bk_product_id,
        as_qt_ms_tsk_atb_split_pct_int,
        source_deleted_flg,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_technology_name,
        bk_as_subtechnology_name,
        offer_name,
        child_sku,
        ss_cd
    FROM {{ source('raw', 'ff_as_quote_milestone_task_atb_hdp') }}
),

transformed_exptrans AS (
    SELECT
    bk_as_quote_num,
    bk_as_quote_milestone_num_int,
    bk_as_quote_milestone_task_num,
    product_key,
    bk_product_id,
    as_qt_ms_tsk_atb_split_pct_int,
    source_deleted_flg,
    bk_as_business_service_name,
    bk_as_architecture_name,
    bk_as_technology_name,
    bk_as_subtechnology_name,
    offer_name,
    child_sku,
    ss_cd,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_quote_milestone_task_atb_hdp
),

final AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        product_key,
        bk_product_id,
        as_qt_ms_tsk_atb_split_pct_int,
        source_deleted_flg,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_technology_name,
        bk_as_subtechnology_name,
        offer_name,
        child_sku,
        ss_cd,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final