{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sol_so_end_cust_retro_ar_p', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SOL_SO_END_CUST_RETRO_AR_P',
        'target_table': 'WI_SO_END_CUST_REV_RETRO_P',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.497625+00:00'
    }
) }}

WITH 

source_wi_sol_end_cust_rev_retro_p_xaas AS (
    SELECT
        ru_exaas_subscription_sol_key,
        endcst_erp_cst_act_loc_use_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'wi_sol_end_cust_rev_retro_p_xaas') }}
),

source_n_so_end_customer_tv AS (
    SELECT
        end_customer_key,
        dd_end_customer_type,
        sk_sales_order_header_id_int,
        ss_cd,
        sales_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_ssp_date,
        start_tv_datetime,
        end_ssp_date,
        end_tv_datetime
    FROM {{ source('raw', 'n_so_end_customer_tv') }}
),

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
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        dd_parent_sales_order_line_key
    FROM {{ source('raw', 'n_sol_end_customer_tv') }}
),

final AS (
    SELECT
        end_customer_key,
        dd_end_customer_type,
        sk_sales_order_header_id_int,
        ss_cd,
        sales_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_ssp_date,
        start_tv_datetime,
        end_ssp_date,
        end_tv_datetime
    FROM source_n_sol_end_customer_tv
)

SELECT * FROM final