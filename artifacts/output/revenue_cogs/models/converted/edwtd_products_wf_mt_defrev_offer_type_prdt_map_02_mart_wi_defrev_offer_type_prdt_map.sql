{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_defrev_offer_type_prdt_map', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_MT_DEFREV_OFFER_TYPE_PRDT_MAP',
        'target_table': 'WI_DEFREV_OFFER_TYPE_PRDT_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.157337+00:00'
    }
) }}

WITH 

source_mt_defrev_offer_type_prdt_map AS (
    SELECT
        product_key,
        defrev_component_name,
        defrev_group_name,
        dfrd_rev_offr_hier_node_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_defrev_offer_type_prdt_map') }}
),

source_wi_defrev_offer_type_prdt_map AS (
    SELECT
        product_key,
        defrev_component_name,
        defrev_group_name,
        dfrd_rev_offr_hier_node_key
    FROM {{ source('raw', 'wi_defrev_offer_type_prdt_map') }}
),

final AS (
    SELECT
        product_key,
        defrev_component_name,
        defrev_group_name,
        dfrd_rev_offr_hier_node_key
    FROM source_wi_defrev_offer_type_prdt_map
)

SELECT * FROM final