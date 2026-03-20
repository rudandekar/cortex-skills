{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_cs_deal_so_line_link_me', 'batch', 'edwtd_services'],
    meta={
        'source_workflow': 'wf_m_MT_CS_DEAL_SO_LINE_LINK_ME',
        'target_table': 'MT_CS_DEAL_SO_LINE_LINK_ME',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.094677+00:00'
    }
) }}

WITH 

source_mt_cs_deal_so_line_link AS (
    SELECT
        sales_order_line_key,
        approved_deal_id,
        bk_product_subgroup_id,
        cs_deal_prdt_sub_group_id,
        bk_deal_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_cs_deal_so_line_link') }}
),

final AS (
    SELECT
        sales_order_line_key,
        approved_deal_id,
        bk_product_subgroup_id,
        cs_deal_prdt_sub_group_id,
        bk_deal_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_attribution_cd
    FROM source_mt_cs_deal_so_line_link
)

SELECT * FROM final