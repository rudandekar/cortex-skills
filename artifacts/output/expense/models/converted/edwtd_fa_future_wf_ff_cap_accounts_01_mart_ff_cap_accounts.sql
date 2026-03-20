{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cap_accounts', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_FF_CAP_ACCOUNTS',
        'target_table': 'FF_CAP_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.724120+00:00'
    }
) }}

WITH 

source_cap_accounts AS (
    SELECT
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
        cm,
        hr,
        pl_account
    FROM {{ source('raw', 'cap_accounts') }}
),

transformed_exp_cap_accounts AS (
    SELECT
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
    cm,
    hr,
    pl_account,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cap_accounts
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
    FROM transformed_exp_cap_accounts
)

SELECT * FROM final