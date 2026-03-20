{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_mt_erp_sales_rep', 'batch', 'edwtd_party'],
    meta={
        'source_workflow': 'wf_m_WI_MT_ERP_SALES_REP',
        'target_table': 'WI_MT_ERP_SALES_REP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.085900+00:00'
    }
) }}

WITH 

source_n_erp_sales_rep_tv AS (
    SELECT
        sales_rep_number,
        start_tv_date,
        end_tv_date,
        erp_sales_rep_type,
        erp_sales_rep_name,
        ru_sales_terr_team_name,
        ru_cisco_worker_party_key,
        ss_code,
        edw_create_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        sales_rep_active_end_dt,
        sales_rep_active_start_dt,
        salesrep_status_cd
    FROM {{ source('raw', 'n_erp_sales_rep_tv') }}
),

final AS (
    SELECT
        sales_rep_num
    FROM source_n_erp_sales_rep_tv
)

SELECT * FROM final