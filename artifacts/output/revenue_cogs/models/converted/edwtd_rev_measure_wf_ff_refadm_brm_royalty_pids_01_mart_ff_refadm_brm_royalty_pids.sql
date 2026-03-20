{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_refadm_brm_royalty_pids', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_FF_REFADM_BRM_ROYALTY_PIDS',
        'target_table': 'FF_REFADM_BRM_ROYALTY_PIDS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.209335+00:00'
    }
) }}

WITH 

source_xxscm_rlt_bom AS (
    SELECT
        top_pid,
        sw_assembly,
        commercial_sw_item,
        agreement_number,
        agreement_index,
        multiplier,
        record_type,
        nat_treatment,
        nat_offering_name,
        product_family,
        bom_id,
        commercial_sw_item_id,
        sw_assembly_item_id,
        top_pid_id,
        bom_change_date,
        n_attribute1,
        n_attribute2,
        n_attribute3,
        n_attribute4,
        n_attribute5,
        v_attribute1,
        v_attribute2,
        v_attribute3,
        v_attribute4,
        v_attribute5,
        d_attribute1,
        d_attribute2,
        d_attribute3,
        d_attribute4,
        d_attribute5,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by
    FROM {{ source('raw', 'xxscm_rlt_bom') }}
),

transformed_exptrans AS (
    SELECT
    top_pid,
    sw_assembly,
    commercial_sw_item,
    agreement_number,
    agreement_index,
    multiplier,
    record_type,
    nat_treatment,
    nat_offering_name,
    product_family,
    n_attribute1,
    n_attribute2,
    n_attribute3,
    n_attribute4,
    n_attribute5,
    v_attribute1,
    v_attribute2,
    v_attribute3,
    v_attribute4,
    v_attribute5,
    d_attribute1,
    d_attribute2,
    d_attribute3,
    d_attribute4,
    d_attribute5
    FROM source_xxscm_rlt_bom
),

final AS (
    SELECT
        top_pid,
        sw_assembly,
        commercial_sw_item,
        agreement_part,
        royalty_index,
        multiplier,
        record_type,
        nat_treatment,
        nat_offering_name,
        product_family,
        n_attribute1,
        n_attribute2,
        n_attribute3,
        n_attribute4,
        n_attribute5,
        v_attribute1,
        v_attribute2,
        v_attribute3,
        v_attribute4,
        v_attribute5,
        d_attribute1,
        d_attribute2,
        d_attribute3,
        d_attribute4,
        d_attribute5,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by
    FROM transformed_exptrans
)

SELECT * FROM final