{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_customs_pricing_method_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOMS_PRICING_METHOD_STG23NF',
        'target_table': 'N_CUSTOMS_PRICING_METHOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.679622+00:00'
    }
) }}

WITH 

source_n_customs_pricing_method AS (
    SELECT
        bk_customs_pricing_method_cd,
        bk_ss_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ep_price_method_id_int
    FROM {{ source('raw', 'n_customs_pricing_method') }}
),

final AS (
    SELECT
        bk_customs_pricing_method_cd,
        bk_ss_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ep_price_method_id_int
    FROM source_n_customs_pricing_method
)

SELECT * FROM final