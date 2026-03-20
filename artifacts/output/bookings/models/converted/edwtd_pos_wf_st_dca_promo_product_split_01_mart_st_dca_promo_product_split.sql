{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dca_promo_product_split', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_DCA_PROMO_PRODUCT_SPLIT',
        'target_table': 'ST_DCA_PROMO_PRODUCT_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.306290+00:00'
    }
) }}

WITH 

source_ff_dca_promo_product_split AS (
    SELECT
        promo_prod_split_id,
        promo_id,
        promo_prod_detail_id,
        sub_promo_type,
        sub_promo_number,
        split_factor,
        required_product,
        split_quantity,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        it_comments,
        sub_promo_desc,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ff_dca_promo_product_split') }}
),

final AS (
    SELECT
        promo_prod_split_id,
        promo_id,
        promo_prod_detail_id,
        sub_promo_type,
        sub_promo_number,
        split_factor,
        required_product,
        split_quantity,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        it_comments,
        sub_promo_desc,
        batch_id,
        action_cd,
        create_datetime
    FROM source_ff_dca_promo_product_split
)

SELECT * FROM final