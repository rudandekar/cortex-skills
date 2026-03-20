{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_hndske_bndle_non_bndle_splt', 'batch', 'edwtd_bndl_attr'],
    meta={
        'source_workflow': 'wf_m_MT_HNDSKE_BNDLE_NON_BNDLE_SPLT',
        'target_table': 'MT_HNDSKE_BNDLE_NON_BNDLE_SPLT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.341676+00:00'
    }
) }}

WITH 

source_n_hndshk_bndl_typ_fmly_splt_tv AS (
    SELECT
        bk_product_family_id,
        bk_handshake_bundle_type_name,
        start_tv_dt,
        end_tv_dt,
        bookings_revenue_split_pct,
        dv_fiscal_year_month_num_int,
        cogs_split_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_hndshk_bndl_typ_fmly_splt_tv') }}
),

final AS (
    SELECT
        handshake_bundle_product_key,
        handshake_bundle_type_name,
        fiscal_year_month_num_int,
        bookings_revenue_split_pct,
        cogs_split_pct,
        pre_item_key,
        post_product_family_id,
        source_product_family_id,
        post_item_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_hndshk_bndl_typ_fmly_splt_tv
)

SELECT * FROM final