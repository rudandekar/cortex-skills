{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sol_end_cust_modified_aph', 'batch', 'edwtd_aph'],
    meta={
        'source_workflow': 'wf_m_WI_SOL_END_CUST_MODIFIED_APH',
        'target_table': 'WI_SOL_END_CUST_MODIFIED_APH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.261999+00:00'
    }
) }}

WITH 

source_n_sol_end_customer_tv AS (
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
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_sol_end_customer_tv') }}
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
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_n_sol_end_customer_tv
)

SELECT * FROM final