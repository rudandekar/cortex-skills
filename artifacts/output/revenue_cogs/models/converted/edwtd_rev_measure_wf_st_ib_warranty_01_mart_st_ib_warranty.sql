{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ib_warranty', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_IB_WARRANTY',
        'target_table': 'ST_IB_WARRANTY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.978127+00:00'
    }
) }}

WITH 

source_ib_warranty_ff AS (
    SELECT
        gpi_business_unit_name,
        gpi_product_family_name,
        gpi_product_code,
        product_type_code,
        item_type_name,
        sudi_flag,
        install_base,
        inw_inc,
        inw_ooc,
        inw_mc,
        oow_inc,
        oow_ooc,
        oow_mc,
        mw_inc,
        mw_ooc,
        mw_mc,
        earliest_good_ib_shipment_date
    FROM {{ source('raw', 'ib_warranty_ff') }}
),

final AS (
    SELECT
        gpi_business_unit_name,
        gpi_product_family_name,
        gpi_product_code,
        product_type_code,
        item_type_name,
        sudi_flag,
        install_base,
        inw_inc,
        inw_ooc,
        inw_mc,
        oow_inc,
        oow_ooc,
        oow_mc,
        mw_inc,
        mw_ooc,
        mw_mc,
        earliest_good_ib_shipment_date
    FROM source_ib_warranty_ff
)

SELECT * FROM final