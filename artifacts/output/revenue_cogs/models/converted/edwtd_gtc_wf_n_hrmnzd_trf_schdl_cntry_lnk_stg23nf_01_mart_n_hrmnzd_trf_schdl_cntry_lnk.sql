{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hrmnzd_trf_schdl_cntry_lnk_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HRMNZD_TRF_SCHDL_CNTRY_LNK_STG23NF',
        'target_table': 'N_HRMNZD_TRF_SCHDL_CNTRY_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.981149+00:00'
    }
) }}

WITH 

source_n_hrmnzd_trf_schdl_cntry_lnk AS (
    SELECT
        bk_hrmnzd_tariff_schedule_cd,
        bk_iso_country_cd,
        bk_hts_effective_start_dt,
        bk_active_flg,
        hts_effective_end_dt,
        custom_hts_flg,
        hts_content_override_flg,
        override_duty_rt,
        duty_rt,
        hts_content_upload_dt,
        hts_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_modified_dtm,
        dv_source_modified_dt,
        dv_hts_cntry_asgnmnt_status_cd,
        dashboard_flg
    FROM {{ source('raw', 'n_hrmnzd_trf_schdl_cntry_lnk') }}
),

final AS (
    SELECT
        bk_hrmnzd_tariff_schedule_cd,
        bk_iso_country_cd,
        bk_hts_effective_start_dt,
        bk_active_flg,
        hts_effective_end_dt,
        custom_hts_flg,
        hts_content_override_flg,
        override_duty_rt,
        duty_rt,
        hts_content_upload_dt,
        hts_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_modified_dtm,
        dv_source_modified_dt,
        dv_hts_cntry_asgnmnt_status_cd,
        dashboard_flg
    FROM source_n_hrmnzd_trf_schdl_cntry_lnk
)

SELECT * FROM final