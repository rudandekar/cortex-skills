{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_sbscrptn_qt_atstb_split_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_SBSCRPTN_QT_ATSTB_SPLIT_HDP',
        'target_table': 'ST_AS_SBSCRPTN_QT_ATSTB_SPLIT_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.531095+00:00'
    }
) }}

WITH 

source_ff_as_sbscrptn_qt_atstb_split_hdp AS (
    SELECT
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        bk_as_technology_name,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_subtechnology_name,
        atstb_usd_cost,
        atstb_net_usd_price,
        atstb_list_usd_price,
        source_deleted_flg,
        ss_cd
    FROM {{ source('raw', 'ff_as_sbscrptn_qt_atstb_split_hdp') }}
),

transformed_exptrans AS (
    SELECT
    bk_deliverable_name,
    bk_service_component_name,
    bk_service_program_name,
    bk_as_quote_num,
    bk_as_technology_name,
    bk_as_business_service_name,
    bk_as_architecture_name,
    bk_as_subtechnology_name,
    atstb_usd_cost,
    atstb_net_usd_price,
    atstb_list_usd_price,
    source_deleted_flg,
    ss_cd,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_sbscrptn_qt_atstb_split_hdp
),

final AS (
    SELECT
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        bk_as_technology_name,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_subtechnology_name,
        atstb_usd_cost,
        atstb_net_usd_price,
        atstb_list_usd_price,
        source_deleted_flg,
        ss_cd,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final