{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_cs_deal_svc_fin_hier_me', 'batch', 'edwtd_services'],
    meta={
        'source_workflow': 'wf_m_MT_CS_DEAL_SVC_FIN_HIER_ME',
        'target_table': 'MT_CS_DEAL_SVC_FIN_HIER_ME',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.331757+00:00'
    }
) }}

WITH 

source_mt_cs_deal_svc_fin_hier_alloc AS (
    SELECT
        bk_product_subgroup_id,
        approved_deal_id,
        bk_service_category_id,
        service_category_description,
        bk_technology_group_id,
        technology_group_code,
        technology_group_description,
        product_subgroup_description,
        bk_allocated_servc_group_id,
        allocation_percentage,
        service_group_description,
        ru_service_brand_code,
        ru_service_level_group_code,
        dv_prdt_sub_grp_alloc_src_cd,
        item_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_service_category_grouping_name,
        ru_bk_acqstn_cmpny_name
    FROM {{ source('raw', 'mt_cs_deal_svc_fin_hier_alloc') }}
),

final AS (
    SELECT
        bk_product_subgroup_id,
        approved_deal_id,
        bk_service_category_id,
        service_category_description,
        bk_technology_group_id,
        technology_group_code,
        technology_group_description,
        product_subgroup_description,
        bk_allocated_servc_group_id,
        allocation_percentage,
        service_group_description,
        ru_service_brand_code,
        ru_service_level_group_code,
        dv_prdt_sub_grp_alloc_src_cd,
        item_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_service_category_grouping_name,
        ru_bk_acqstn_cmpny_name
    FROM source_mt_cs_deal_svc_fin_hier_alloc
)

SELECT * FROM final