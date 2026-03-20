{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_sites_d', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_SITES_D',
        'target_table': 'ST_TSS_SITES_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.339397+00:00'
    }
) }}

WITH 

source_ff_tss_sites_d AS (
    SELECT
        party_number,
        party_site_id,
        party_id,
        bl_site_key,
        party_name,
        address1,
        address2,
        address3,
        address4,
        city,
        state,
        country,
        postal_code,
        theater,
        time_zone,
        ship_to_flag,
        creation_date,
        pst_creation_date,
        bl_last_update_date,
        cust_account_id,
        cust_acct_site_id,
        cust_theater,
        cust_country
    FROM {{ source('raw', 'ff_tss_sites_d') }}
),

transformed_exp_tss_sites_d AS (
    SELECT
    party_number,
    party_site_id,
    party_id,
    bl_site_key,
    party_name,
    address1,
    address2,
    address3,
    address4,
    city,
    state,
    country,
    postal_code,
    theater,
    time_zone,
    ship_to_flag,
    creation_date,
    pst_creation_date,
    bl_last_update_date,
    cust_account_id,
    cust_acct_site_id,
    cust_theater,
    cust_country,
    IFF(LTRIM(RTRIM(PARTY_NUMBER)) = '\N',NULL,PARTY_NUMBER) AS party_number1,
    IFF(LTRIM(RTRIM(PARTY_SITE_ID)) = '\N',NULL,PARTY_SITE_ID) AS party_site_id1,
    IFF(LTRIM(RTRIM(PARTY_ID)) = '\N',NULL,PARTY_ID) AS party_id1,
    IFF(LTRIM(RTRIM(BL_SITE_KEY)) = '\N',NULL,BL_SITE_KEY) AS bl_site_key1,
    IFF(LTRIM(RTRIM(PARTY_NAME)) = '\N',NULL,PARTY_NAME) AS party_name1,
    IFF(LTRIM(RTRIM(ADDRESS1)) = '\N',NULL,ADDRESS1) AS address11,
    IFF(LTRIM(RTRIM(ADDRESS2)) = '\N',NULL,ADDRESS2) AS address21,
    IFF(LTRIM(RTRIM(ADDRESS3)) = '\N',NULL,ADDRESS3) AS address31,
    IFF(LTRIM(RTRIM(ADDRESS4)) = '\N',NULL,ADDRESS4) AS address41,
    IFF(LTRIM(RTRIM(CITY)) = '\N',NULL,CITY) AS city1,
    IFF(LTRIM(RTRIM(STATE)) = '\N',NULL,STATE) AS state1,
    IFF(LTRIM(RTRIM(COUNTRY)) = '\N',NULL,COUNTRY) AS country1,
    IFF(LTRIM(RTRIM(POSTAL_CODE)) = '\N',NULL,POSTAL_CODE) AS postal_code1,
    IFF(LTRIM(RTRIM(THEATER)) = '\N',NULL,THEATER) AS theater1,
    IFF(LTRIM(RTRIM(TIME_ZONE)) = '\N',NULL,TIME_ZONE) AS time_zone1,
    IFF(LTRIM(RTRIM(SHIP_TO_FLAG)) = '\N',NULL,SHIP_TO_FLAG) AS ship_to_flag1,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',NULL,TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS creation_date1,
    IFF(LTRIM(RTRIM(PST_CREATION_DATE)) = '\N',NULL,TO_DATE(PST_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS pst_creation_date1,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS bl_last_update_date1,
    IFF(LTRIM(RTRIM(CUST_ACCOUNT_ID)) = '\N',NULL,TO_BIGINT(CUST_ACCOUNT_ID)) AS cust_account_id1,
    IFF(LTRIM(RTRIM(CUST_ACCT_SITE_ID)) = '\N',NULL,TO_BIGINT(CUST_ACCT_SITE_ID)) AS cust_acct_site_id1,
    IFF(LTRIM(RTRIM(CUST_THEATER)) = '\N',NULL,CUST_THEATER) AS cust_theater1,
    IFF(LTRIM(RTRIM(CUST_COUNTRY)) = '\N',NULL,CUST_COUNTRY) AS cust_country1
    FROM source_ff_tss_sites_d
),

final AS (
    SELECT
        party_number,
        party_site_id,
        party_id,
        bl_site_key,
        party_name,
        address1,
        address2,
        address3,
        address4,
        city,
        state,
        country,
        postal_code,
        theater,
        time_zone,
        ship_to_flag,
        creation_date,
        pst_creation_date,
        bl_last_update_date,
        cust_account_id,
        cust_acct_site_id,
        cust_theater,
        cust_country
    FROM transformed_exp_tss_sites_d
)

SELECT * FROM final