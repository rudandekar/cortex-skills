{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sol_end_customer ', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SOL_END_CUSTOMER ',
        'target_table': 'W_SOL_END_CUSTOMER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.964403+00:00'
    }
) }}

WITH 

source_wi_sol_end_customer AS (
    SELECT
        sales_order_line_key,
        end_customer_key,
        end_customer_type_code,
        ss_code,
        end_customer_assgn_level,
        edw_create_datetime,
        ru_parnt_sls_order_line_key,
        end_cust_erp_cntct_prty_key
    FROM {{ source('raw', 'wi_sol_end_customer') }}
),

final AS (
    SELECT
        sales_order_line_key,
        end_customer_key,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        end_customer_type_code,
        ss_code,
        end_customer_assgn_level,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        dd_parent_sales_order_line_key,
        end_cust_erp_cntct_prty_key,
        action_code,
        dml_type
    FROM source_wi_sol_end_customer
)

SELECT * FROM final