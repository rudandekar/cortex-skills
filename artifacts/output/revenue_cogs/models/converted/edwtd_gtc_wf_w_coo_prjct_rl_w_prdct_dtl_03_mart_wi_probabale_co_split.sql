{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_coo_prjct_rl_w_prdct_dtl', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_COO_PRJCT_RL_W_PRDCT_DTL',
        'target_table': 'WI_PROBABALE_CO_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.587210+00:00'
    }
) }}

WITH 

source_st_co_pid_bw_data_intf AS (
    SELECT
        batch_id,
        pid_bw_data_id,
        pid,
        invalid_country,
        probabale_co,
        product_family,
        bu,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        status,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_co_pid_bw_data_intf') }}
),

source_wi_probabale_co_split AS (
    SELECT
        batch_id,
        pid_bw_data_id,
        pid,
        probabale_co
    FROM {{ source('raw', 'wi_probabale_co_split') }}
),

source_w_coo_prjct_rl_w_prdct_dtl AS (
    SELECT
        product_key,
        bk_invalid_country_cd,
        dtl_sts_cd,
        probable_country_cd,
        dtl_create_dt,
        dtl_last_update_dt,
        dtl_last_upd_csco_wrkr_pty_key,
        dtl_created_csco_wrkr_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_coo_prjct_rl_w_prdct_dtl') }}
),

source_ex_co_pid_bw_data_intf AS (
    SELECT
        batch_id,
        pid_bw_data_id,
        pid,
        invalid_country,
        probabale_co,
        product_family,
        bu,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        status,
        exception_cd,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_co_pid_bw_data_intf') }}
),

source_wi_invalid_country_split AS (
    SELECT
        batch_id,
        pid_bw_data_id,
        pid,
        invalid_country
    FROM {{ source('raw', 'wi_invalid_country_split') }}
),

final AS (
    SELECT
        batch_id,
        pid_bw_data_id,
        pid,
        probabale_co
    FROM source_wi_invalid_country_split
)

SELECT * FROM final