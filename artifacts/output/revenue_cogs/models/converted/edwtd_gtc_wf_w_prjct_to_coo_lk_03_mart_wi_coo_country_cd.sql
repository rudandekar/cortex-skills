{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_prjct_to_coo_lk', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_PRJCT_TO_COO_LK',
        'target_table': 'WI_COO_COUNTRY_CD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.157111+00:00'
    }
) }}

WITH 

source_wi_coo_probableco AS (
    SELECT
        projectid,
        projectname,
        probableco
    FROM {{ source('raw', 'wi_coo_probableco') }}
),

source_wi_coo_country_cd AS (
    SELECT
        projectid,
        projectname,
        coo
    FROM {{ source('raw', 'wi_coo_country_cd') }}
),

source_st_co_report AS (
    SELECT
        batch_id,
        projectid,
        projectname,
        productfamily,
        pidname,
        boardbuildorg,
        fatorg,
        dforg,
        rule,
        anscommasaparated,
        projectassignedto,
        createdby,
        projectlastupdatedby,
        lastupdateddate,
        notes,
        snprefix,
        coo,
        ruleassgntype,
        probableco,
        validtaacountryflag,
        validtaacountry,
        process,
        recordtype,
        rulew,
        ruletype,
        projectstatus,
        reasonforinactivation,
        manualpidinfo,
        dtrevisionid,
        obsoletepidcount,
        pidstatus,
        inactivelocs,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_co_report') }}
),

source_w_prjct_to_coo_lk AS (
    SELECT
        bk_iso_country_cd,
        bk_coo_prjct_name,
        src_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_prjct_to_coo_lk') }}
),

source_ex_co_report AS (
    SELECT
        batch_id,
        projectid,
        projectname,
        productfamily,
        pidname,
        boardbuildorg,
        fatorg,
        dforg,
        rule,
        anscommasaparated,
        projectassignedto,
        createdby,
        projectlastupdatedby,
        lastupdateddate,
        notes,
        snprefix,
        coo,
        ruleassgntype,
        probableco,
        validtaacountryflag,
        validtaacountry,
        process,
        recordtype,
        rulew,
        ruletype,
        projectstatus,
        reasonforinactivation,
        manualpidinfo,
        dtrevisionid,
        obsoletepidcount,
        pidstatus,
        inactivelocs,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_co_report') }}
),

final AS (
    SELECT
        projectid,
        projectname,
        coo
    FROM source_ex_co_report
)

SELECT * FROM final