{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_pnl_ascogs_dpt_acct_mpg', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_APSP_PNL_ASCOGS_DPT_ACCT_MPG',
        'target_table': 'FF_TGT_REV_DEFAULT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.501905+00:00'
    }
) }}

WITH 

source_w_apsp_pnl_ascogs_dpt_acct_mpg AS (
    SELECT
        bk_department_cd,
        bk_company_cd,
        bk_pnl_measure_name,
        bk_fiscal_year_month_num_int,
        bk_financial_account_cd,
        financial_acct_reporting_flg,
        allocation_method_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_pnl_ascogs_dpt_acct_mpg') }}
),

source_ae_ascogs_mra_struct_map AS (
    SELECT
        pnl_measure_name,
        pl_node_value,
        pl_node_level,
        gl_account_code,
        account_include_flag,
        allocation_method
    FROM {{ source('raw', 'ae_ascogs_mra_struct_map') }}
),

sorted_srttrans AS (
    SELECT *
    FROM source_ae_ascogs_mra_struct_map
    ORDER BY 1
),

transformed_exptrans AS (
    SELECT
    fiscal_month_id,
    pnl_measure_name,
    pl_node_value,
    pl_node_level,
    gl_account_code,
    account_include_flag,
    allocation_method_name,
    IFF(PL_NODE_LEVEL<to_integer(10),'0'||to_char(PL_NODE_LEVEL),to_char(PL_NODE_LEVEL)) AS pl_node_level_int,
    PL_NODE_LEVEL_INT AS pl_node_level_int_field,
    'NODE_LEVEL'||PL_NODE_LEVEL_INT||'_VALUE' AS out_pl_node_level_int
    FROM sorted_srttrans
),

transformed_exptrans1 AS (
    SELECT
    fiscal_month_id,
    gl_account_code,
    pnl_measure_name,
    account_include_flag,
    out_pl_node_level_int,
    pl_node_value,
    pl_node_level_int_field,
    allocation_method_name,
    PNL_MEASURE_NAME AS pnl_measure_name_var,
    PL_NODE_VALUE AS pl_node_value_out,
    ''||chr(39)||PNL_MEASURE_NAME_VAR||chr(39)||', '||chr(39)||ACCOUNT_INCLUDE_FLAG||chr(39)||' , '||chr(39)||GL_ACCOUNT_CODE||chr(39)||', '||chr(39)||PL_NODE_VALUE||chr(39)||' , '||chr(39)||PL_NODE_LEVEL_INT_FIELD||chr(39)|| ' , ' || chr(39) || FISCAL_MONTH_ID || chr(39) ||' , ' || chr(39)|| ALLOCATION_METHOD_NAME || chr(39) || ' FROM '||'FINLGLVWDB'||'.R_PL_HIERARCHY PL WHERE '||OUT_PL_NODE_LEVEL_INT||'='||chr(39)||PL_NODE_VALUE_OUT||chr(39) AS sql_out,
    'SELECT PL.FINANCIAL_COMPANY_CODE||PL.FINANCIAL_DEPARTMENT_CODE,' AS sql_out1
    FROM transformed_exptrans
),

transformed_exptrans4 AS (
    SELECT
    financial_department_code,
    pnl_measure_name,
    account_include_flag_out,
    gl_account_code,
    LTRIM(RTRIM(PNL_MEASURE_NAME)) AS pnl_measure_name_out,
    CONCAT(CONCAT(FINANCIAL_DEPARTMENT_CODE,GL_ACCOUNT_CODE),ACCOUNT_INCLUDE_FLAG_OUT) AS dept_number_flag
    FROM transformed_exptrans1
),

final AS (
    SELECT
        tgt_default_column
    FROM transformed_exptrans4
)

SELECT * FROM final