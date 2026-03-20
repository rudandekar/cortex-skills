{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_sbscrptn_qt_atstb_split_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_AS_SBSCRPTN_QT_ATSTB_SPLIT_SEP',
        'target_table': 'W_AS_SBSCRP_QT_ATSTB_SPLIT_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.789380+00:00'
    }
) }}

WITH 

source_w_as_sbscrp_qt_atstb_split_sep AS (
    SELECT
        atstb_split_key,
        atstb_usd_cost,
        atstb_net_usd_price,
        atstb_list_usd_price,
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        bk_as_technology_name,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_subtechnology_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_sbscrp_qt_atstb_split_sep') }}
),

final AS (
    SELECT
        atstb_split_key,
        atstb_usd_cost,
        atstb_net_usd_price,
        atstb_list_usd_price,
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        bk_as_technology_name,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_subtechnology_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        action_code,
        dml_type
    FROM source_w_as_sbscrp_qt_atstb_split_sep
)

SELECT * FROM final