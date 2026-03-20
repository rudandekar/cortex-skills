{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_mt_as_aot_to_be_mapping', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_MT_AS_AOT_TO_BE_MAPPING',
        'target_table': 'WI_MT_AS_AOT_TO_BE_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.168022+00:00'
    }
) }}

WITH 

source_wi_mt_as_aot_to_be_mapping AS (
    SELECT
        l3_sales_territory_name_code,
        dv_bk_as_ato_archi_name,
        dv_bk_busi_svc_offer_type_name,
        dv_bk_as_ato_tech_name,
        bk_sub_business_entity_name,
        bk_business_entity_name,
        dv_allocation_percentage,
        allocation_flg
    FROM {{ source('raw', 'wi_mt_as_aot_to_be_mapping') }}
),

final AS (
    SELECT
        l3_sales_territory_name_code,
        dv_bk_as_ato_archi_name,
        dv_bk_busi_svc_offer_type_name,
        dv_bk_as_ato_tech_name,
        bk_sub_business_entity_name,
        bk_business_entity_name,
        dv_allocation_percentage,
        allocation_flg
    FROM source_wi_mt_as_aot_to_be_mapping
)

SELECT * FROM final