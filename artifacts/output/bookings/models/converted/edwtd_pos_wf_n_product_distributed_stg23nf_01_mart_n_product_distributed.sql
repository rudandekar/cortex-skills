{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_product_distributed_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_PRODUCT_DISTRIBUTED_STG23NF',
        'target_table': 'N_PRODUCT_DISTRIBUTED',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.533402+00:00'
    }
) }}

WITH 

source_st_tbl_product_id_mapping1 AS (
    SELECT
        product_id,
        iot_flag,
        cisco_one_flag,
        fast_track_flag,
        sbtg_flag
    FROM {{ source('raw', 'st_tbl_product_id_mapping1') }}
),

final AS (
    SELECT
        product_key,
        internet_of_things_cd,
        cisco_one_flg,
        fast_track_flg,
        small_business_tech_grp_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tbl_product_id_mapping1
)

SELECT * FROM final