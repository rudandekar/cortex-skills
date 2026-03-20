{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_co_report', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_CO_REPORT',
        'target_table': 'FF_CO_REPORT_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.225151+00:00'
    }
) }}

WITH 

source_co_report AS (
    SELECT
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
        inactivelocs
    FROM {{ source('raw', 'co_report') }}
),

transformed_exptrans AS (
    SELECT
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
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_co_report
),

final AS (
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
    FROM transformed_exptrans
)

SELECT * FROM final