{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_ff_fin_svc_goods_map_tmplt', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_FF_FIN_SVC_GOODS_MAP_TMPLT',
        'target_table': 'FF_FIN_SVC_GOODS_MAP_TMPLT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.131018+00:00'
    }
) }}

WITH 

source_fin_svc_goods_map_tmplt AS (
    SELECT
        fiscal_month_id,
        data_source,
        source_name,
        svc_product_id,
        sub_business_entity_name,
        product_id,
        split_percentage,
        comments
    FROM {{ source('raw', 'fin_svc_goods_map_tmplt') }}
),

final AS (
    SELECT
        fiscal_month_id,
        data_source,
        source_name,
        svc_product_id,
        sub_business_entity_name,
        product_id,
        split_percentage,
        comments
    FROM source_fin_svc_goods_map_tmplt
)

SELECT * FROM final