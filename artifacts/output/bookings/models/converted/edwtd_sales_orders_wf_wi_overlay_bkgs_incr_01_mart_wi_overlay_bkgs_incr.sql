{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_overlay_bkgs_incr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_OVERLAY_BKGS_INCR',
        'target_table': 'WI_OVERLAY_BKGS_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.404259+00:00'
    }
) }}

WITH 

source_wi_overlay_bkgs_incr AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        org_id,
        edw_create_dtm,
        edw_create_user,
        profile_chk_id,
        sales_territory_key,
        sales_rep_num
    FROM {{ source('raw', 'wi_overlay_bkgs_incr') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        org_id,
        edw_create_dtm,
        edw_create_user,
        profile_chk_id,
        sales_territory_key,
        sales_rep_num
    FROM source_wi_overlay_bkgs_incr
)

SELECT * FROM final