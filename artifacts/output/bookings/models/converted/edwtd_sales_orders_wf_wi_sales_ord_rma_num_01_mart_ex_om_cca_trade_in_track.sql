{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_ord_rma_num', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_ORD_RMA_NUM',
        'target_table': 'EX_OM_CCA_TRADE_IN_TRACK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.758593+00:00'
    }
) }}

WITH 

source_st_om_cca_trade_in_track AS (
    SELECT
        business_unit,
        created_by,
        creation_date,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        lob,
        product_bulletin_number,
        program_name,
        quote_number,
        rma_number,
        trade_in_order_number,
        trade_in_track_id,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cca_trade_in_track') }}
),

final AS (
    SELECT
        business_unit,
        created_by,
        creation_date,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        lob,
        product_bulletin_number,
        program_name,
        quote_number,
        rma_number,
        trade_in_order_number,
        trade_in_track_id,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM source_st_om_cca_trade_in_track
)

SELECT * FROM final