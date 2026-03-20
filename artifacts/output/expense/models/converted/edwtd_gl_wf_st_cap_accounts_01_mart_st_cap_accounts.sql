{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cap_accounts', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CAP_ACCOUNTS',
        'target_table': 'ST_CAP_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.696865+00:00'
    }
) }}

WITH 

source_ff_cap_accounts AS (
    SELECT
        batch_id,
        depreciable_life,
        read_only,
        description,
        account_type,
        comments,
        gl_account_map,
        parent_acct_id,
        account_id,
        ca,
        it,
        wpr,
        mfg,
        cdo,
        sales,
        cap_account_id,
        dep_account_id,
        line_account,
        ui_tab,
        tab_display_flag,
        cip_flag,
        rollup_flag,
        catagory,
        upload_flag,
        fi,
        cm_flag,
        hr,
        pl_account,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cap_accounts') }}
),

final AS (
    SELECT
        batch_id,
        depreciable_life,
        read_only,
        description,
        account_type,
        comments,
        gl_account_map,
        parent_acct_id,
        account_id,
        ca,
        it,
        wpr,
        mfg,
        cdo,
        sales,
        cap_account_id,
        dep_account_id,
        line_account,
        ui_tab,
        tab_display_flag,
        cip_flag,
        rollup_flag,
        catagory,
        upload_flag,
        fi,
        cm_flag,
        hr,
        pl_account,
        create_datetime,
        action_code
    FROM source_ff_cap_accounts
)

SELECT * FROM final