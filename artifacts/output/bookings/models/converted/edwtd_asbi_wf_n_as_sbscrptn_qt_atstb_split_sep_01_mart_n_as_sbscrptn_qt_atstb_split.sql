{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_sbscrptn_qt_atstb_split_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_SBSCRPTN_QT_ATSTB_SPLIT_SEP',
        'target_table': 'N_AS_SBSCRPTN_QT_ATSTB_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.361407+00:00'
    }
) }}

WITH 

source_n_as_sbscrptn_qt_atstb_split AS (
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
        ss_cd
    FROM {{ source('raw', 'n_as_sbscrptn_qt_atstb_split') }}
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
        ss_cd
    FROM source_n_as_sbscrptn_qt_atstb_split
)

SELECT * FROM final