{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_co_report', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CO_REPORT',
        'target_table': 'ST_CO_REPORT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.653156+00:00'
    }
) }}

WITH 

source_ff_co_report_incr AS (
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
    FROM {{ source('raw', 'ff_co_report_incr') }}
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
    FROM source_ff_co_report_incr
)

SELECT * FROM final