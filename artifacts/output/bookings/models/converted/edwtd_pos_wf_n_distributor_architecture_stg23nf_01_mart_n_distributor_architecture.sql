{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_distributor_architecture_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTRIBUTOR_ARCHITECTURE_STG23NF',
        'target_table': 'N_DISTRIBUTOR_ARCHITECTURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.867622+00:00'
    }
) }}

WITH 

source_st_tbl_disti_arch_mapping1 AS (
    SELECT
        product_family,
        internal_sub_business_entity,
        internal_business_entity,
        mapping,
        disti_arch,
        disti_sub_arch,
        technologies
    FROM {{ source('raw', 'st_tbl_disti_arch_mapping1') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_sub_business_entity_name,
        bk_business_entity_type_cd,
        bk_business_entity_name,
        allocation_factor_cd,
        distri_architecture_name,
        distri_sub_architecture_name,
        distri_technology_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tbl_disti_arch_mapping1
)

SELECT * FROM final