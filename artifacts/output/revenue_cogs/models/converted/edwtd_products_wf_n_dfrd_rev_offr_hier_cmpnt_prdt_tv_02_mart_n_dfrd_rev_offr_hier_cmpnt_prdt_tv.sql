{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_dfrd_rev_offr_hier_cmpnt_prdt_tv', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_N_DFRD_REV_OFFR_HIER_CMPNT_PRDT_TV',
        'target_table': 'N_DFRD_REV_OFFR_HIER_CMPNT_PRDT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.886675+00:00'
    }
) }}

WITH 

source_n_dfrd_rev_offr_hier_cmpnt_prdt AS (
    SELECT
        product_key,
        dfrd_rev_offr_hier_node_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_dfrd_rev_offr_hier_cmpnt_prdt') }}
),

source_n_dfrd_rev_offr_hier_cmpnt_prdt_tv AS (
    SELECT
        product_key,
        dfrd_rev_offr_hier_node_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_dfrd_rev_offr_hier_cmpnt_prdt_tv') }}
),

final AS (
    SELECT
        product_key,
        dfrd_rev_offr_hier_node_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM source_n_dfrd_rev_offr_hier_cmpnt_prdt_tv
)

SELECT * FROM final