{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_bookings_sbase_sales_alloc', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_R_BOOKINGS_SBASE_SALES_ALLOC',
        'target_table': 'R_BOOKINGS_SBASE_SALES_ALLOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.636491+00:00'
    }
) }}

WITH 

source_r_bookings_sbase_corp_alloc AS (
    SELECT
        tech_group,
        trans_fiscal_month,
        product_sub_group,
        salesrep,
        fiscal_year,
        measure,
        proc_fiscal_week,
        trans_fiscal_week,
        tms_level4,
        bkg_type,
        sub_sc,
        country,
        chanell_type,
        partner_sub_type,
        part_cert,
        partner_global_name,
        bk_arch_play_hier_node_cd,
        book_net,
        book_list,
        book_cost,
        book_qty,
        hier_type
    FROM {{ source('raw', 'r_bookings_sbase_corp_alloc') }}
),

final AS (
    SELECT
        tech_group,
        trans_fiscal_month,
        product_sub_group,
        salesrep,
        fiscal_year,
        measure,
        proc_fiscal_week,
        trans_fiscal_week,
        tms_level4,
        bkg_type,
        sub_sc,
        country,
        chanell_type,
        partner_sub_type,
        part_cert,
        partner_global_name,
        bk_arch_play_hier_node_cd,
        book_net,
        book_list,
        book_cost,
        book_qty,
        hier_type
    FROM source_r_bookings_sbase_corp_alloc
)

SELECT * FROM final