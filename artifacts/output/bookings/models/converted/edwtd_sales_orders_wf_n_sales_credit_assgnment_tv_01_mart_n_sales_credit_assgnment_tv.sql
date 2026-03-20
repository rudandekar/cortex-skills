{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_credit_assgnment_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_CREDIT_ASSGNMENT_TV',
        'target_table': 'N_SALES_CREDIT_ASSGNMENT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.401448+00:00'
    }
) }}

WITH 

source_w_sales_credit_assgnment AS (
    SELECT
        sales_credit_asgn_key,
        sales_order_line_key,
        bk_sales_rep_number,
        bk_sales_credit_type_code,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        sales_commission_percentage,
        sca_source_type_code,
        sales_territory_key,
        ss_code,
        sk_line_seq_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        sk_sc_agent_id_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_credit_assgnment') }}
),

final AS (
    SELECT
        sales_credit_asgn_key,
        sales_order_line_key,
        bk_sales_rep_number,
        bk_sales_credit_type_code,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        sales_commission_percentage,
        sca_source_type_code,
        sales_territory_key,
        ss_code,
        sk_line_seq_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        sk_sc_agent_id_int
    FROM source_w_sales_credit_assgnment
)

SELECT * FROM final