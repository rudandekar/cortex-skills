{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gpf_plng_gp_tan_mpg_tv', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_GPF_PLNG_GP_TAN_MPG_TV',
        'target_table': 'N_GPF_PLNG_GP_TAN_MPG_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.636383+00:00'
    }
) }}

WITH 

source_n_gpf_plng_gp_tan_mpg_tv AS (
    SELECT
        goods_product_key,
        tan_part_key,
        inventory_orgn_name_key,
        tan_per_product_cnt,
        buyer_cisco_worker_party_key,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_gpf_plng_gp_tan_mpg_tv') }}
),

source_w_gpf_plng_gp_tan_mpg AS (
    SELECT
        goods_product_key,
        tan_part_key,
        inventory_orgn_name_key,
        tan_per_product_cnt,
        buyer_cisco_worker_party_key,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gpf_plng_gp_tan_mpg') }}
),

final AS (
    SELECT
        goods_product_key,
        tan_part_key,
        inventory_orgn_name_key,
        tan_per_product_cnt,
        buyer_cisco_worker_party_key,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM source_w_gpf_plng_gp_tan_mpg
)

SELECT * FROM final