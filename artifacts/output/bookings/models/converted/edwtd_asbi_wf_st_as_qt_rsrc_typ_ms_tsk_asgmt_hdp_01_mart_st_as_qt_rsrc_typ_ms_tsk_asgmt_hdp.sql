{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_qt_rsrc_typ_ms_tsk_asgmt_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QT_RSRC_TYP_MS_TSK_ASGMT_HDP',
        'target_table': 'ST_AS_QT_RSRC_TYP_MS_TSK_ASGMT_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.805635+00:00'
    }
) }}

WITH 

source_ff_as_qt_rsrc_typ_ms_tsk_asgmt_hdp AS (
    SELECT
        resource_assignment_key,
        bk_resource_type,
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        sk_task_resource_id_int,
        lump_sum_usd_amt,
        source_deleted_flg,
        ru_num_of_nights_cnt,
        ru_num_of_trips_cnt,
        ru_travel_hours_cnt,
        ru_non_labor_usd_cost,
        ru_work_hours_cnt,
        ru_job_role_cd,
        ru_delivery_group_name,
        ru_partner_name,
        ru_third_party_name,
        resource_usd_cost,
        travel_and_expense_usd_cost,
        ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM {{ source('raw', 'ff_as_qt_rsrc_typ_ms_tsk_asgmt_hdp') }}
),

transformed_exptrans AS (
    SELECT
    resource_assignment_key,
    bk_resource_type,
    bk_as_quote_num,
    bk_as_quote_milestone_num_int,
    bk_as_quote_milestone_task_num,
    sk_task_resource_id_int,
    lump_sum_usd_amt,
    source_deleted_flg,
    ru_num_of_nights_cnt,
    ru_num_of_trips_cnt,
    ru_travel_hours_cnt,
    ru_non_labor_usd_cost,
    ru_work_hours_cnt,
    ru_job_role_cd,
    ru_delivery_group_name,
    ru_partner_name,
    ru_third_party_name,
    resource_usd_cost,
    travel_and_expense_usd_cost,
    ss_cd,
    onsite_hrs_cnt,
    remote_hrs_cnt,
    REPLACESTR(1,RU_PARTNER_NAME,',',' ') AS o_ru_partner_name,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_qt_rsrc_typ_ms_tsk_asgmt_hdp
),

final AS (
    SELECT
        resource_assignment_key,
        bk_resource_type,
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        sk_task_resource_id_int,
        lump_sum_usd_amt,
        source_deleted_flg,
        ru_num_of_nights_cnt,
        ru_num_of_trips_cnt,
        ru_travel_hours_cnt,
        ru_non_labor_usd_cost,
        ru_work_hours_cnt,
        ru_job_role_cd,
        ru_delivery_group_name,
        ru_partner_name,
        ru_third_party_name,
        resource_usd_cost,
        travel_and_expense_usd_cost,
        ss_cd,
        created_dtm,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM transformed_exptrans
)

SELECT * FROM final