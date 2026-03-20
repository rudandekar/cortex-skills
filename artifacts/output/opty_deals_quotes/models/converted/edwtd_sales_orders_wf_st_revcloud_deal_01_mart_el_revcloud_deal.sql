{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_revcloud_deal', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_REVCLOUD_DEAL',
        'target_table': 'EL_REVCLOUD_DEAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.903159+00:00'
    }
) }}

WITH 

source_el_revcloud_deal AS (
    SELECT
        deal_id,
        edw_deal_object_id,
        deal_type,
        deal_status_desc,
        edw_create_dtm,
        edw_update_dtm,
        opty_id
    FROM {{ source('raw', 'el_revcloud_deal') }}
),

source_st_revcloud_deal AS (
    SELECT
        deal_id,
        deal_type,
        deal_status_desc,
        cr_id,
        edw_create_dtm,
        created_by,
        created_on,
        updated_by,
        updated_on,
        opty_id,
        deal_name,
        acct_mngr,
        role_name,
        share_node_name,
        share_node_id,
        comp_product,
        comp_tss,
        comp_as_sub,
        comp_ast
    FROM {{ source('raw', 'st_revcloud_deal') }}
),

final AS (
    SELECT
        deal_id,
        edw_deal_object_id,
        deal_type,
        deal_status_desc,
        edw_create_dtm,
        edw_update_dtm,
        opty_id
    FROM source_st_revcloud_deal
)

SELECT * FROM final