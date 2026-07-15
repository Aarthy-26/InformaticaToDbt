=============================================
-- Author: Ascendion AAVA
-- Created on:
-- Description: Customer data load with address lookup and data quality transformations
=============================================

-- =========================================================
-- CONVERSION LOG
-- =========================================================
-- Input Type : Informatica Mapping (m_customer_data_load)
-- Target Platform : dbt (SQL Model)
-- Conversion Approach :
-- - Translated Informatica source qualifier (SQ_CUSTOMER_MASTER) into dbt {{ source() }} reference
-- - Converted Expression transformations (EXP_CUSTOMER_TRANSFORM) into SELECT with CASE statements
-- - Replaced Lookup transformation (LKP_ADDRESS) with LEFT JOIN subquery
-- - Converted Filter transformation (FLT_VALID_CUSTOMERS) into WHERE clause
-- - Replaced Informatica built-in functions with SQL equivalents:
--   * IIF → CASE WHEN
--   * DECODE → CASE WHEN
--   * NVL → COALESCE
--   * TRUNC → DATE_TRUNC / CAST
--   * SUBSTR → SUBSTRING
--   * SYSDATE → CURRENT_TIMESTAMP
-- - Converted Update Strategy transformation into dbt incremental model with is_incremental() logic
-- - Added unique_key for merge/upsert behavior
-- - Structured as CTE-based pipeline for maintainability
-- Major Risks / Checks :
-- - Validate datatype mappings between source columns and target warehouse types
-- - Validate NULL handling in COALESCE and CASE expressions matches Informatica behavior
-- - Validate incremental logic (DD_UPDATE=1 for UPDATE, DD_INSERT=0 for INSERT) maps correctly to dbt merge
-- - Validate lookup join condition and NULL behavior matches Informatica LKP_ADDRESS transformation
-- - Validate filter condition logic matches FLT_VALID_CUSTOMERS
-- - Verify date/timestamp functions produce identical results across warehouse platforms
-- - Test companion schema.yml for data quality checks (not_null, unique, relationships)
-- =========================================================

{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    on_schema_change='fail'
) }}

-- Reason: Informatica Source Qualifier converted to dbt source reference
with source_customer as (
    select
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        ADDRESS_ID,
        STATUS,
        REGISTRATION_DATE,
        LAST_MODIFIED_DATE
    from {{ source('raw_crm', 'customer_master') }}
    {% if is_incremental() %}
    -- Reason: Incremental filter to process only new/updated records since last run
    where LAST_MODIFIED_DATE > (select max(LAST_MODIFIED_DATE) from {{ this }})
    {% endif %}
),

-- Reason: Informatica Lookup transformation converted to LEFT JOIN with subquery
lookup_address as (
    select
        ADDRESS_ID,
        STREET,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTRY
    from {{ source('raw_crm', 'address_master') }}
),

-- Reason: Informatica Expression transformation converted to SELECT with CASE statements
transformed_customer as (
    select
        sc.CUSTOMER_ID,
        -- Reason: IIF replaced with CASE WHEN for NULL handling
        case 
            when sc.FIRST_NAME is null then 'UNKNOWN'
            else upper(trim(sc.FIRST_NAME))
        end as FIRST_NAME,
        case 
            when sc.LAST_NAME is null then 'UNKNOWN'
            else upper(trim(sc.LAST_NAME))
        end as LAST_NAME,
        -- Reason: NVL replaced with COALESCE
        coalesce(lower(trim(sc.EMAIL)), 'noemail@unknown.com') as EMAIL,
        -- Reason: DECODE replaced with CASE WHEN for phone formatting
        case 
            when sc.PHONE is null then 'NO-PHONE'
            when length(trim(sc.PHONE)) < 10 then 'INVALID-PHONE'
            else trim(sc.PHONE)
        end as PHONE,
        sc.ADDRESS_ID,
        -- Reason: Lookup join to retrieve address details
        coalesce(la.STREET, 'UNKNOWN') as STREET,
        coalesce(la.CITY, 'UNKNOWN') as CITY,
        coalesce(la.STATE, 'UNKNOWN') as STATE,
        coalesce(la.ZIP_CODE, '00000') as ZIP_CODE,
        coalesce(la.COUNTRY, 'UNKNOWN') as COUNTRY,
        -- Reason: DECODE replaced with CASE WHEN for status standardization
        case 
            when sc.STATUS = 'A' then 'ACTIVE'
            when sc.STATUS = 'I' then 'INACTIVE'
            when sc.STATUS = 'S' then 'SUSPENDED'
            else 'UNKNOWN'
        end as STATUS,
        -- Reason: TRUNC replaced with DATE_TRUNC for date normalization
        date_trunc('day', sc.REGISTRATION_DATE) as REGISTRATION_DATE,
        sc.LAST_MODIFIED_DATE,
        -- Reason: SYSDATE replaced with CURRENT_TIMESTAMP
        current_timestamp as LOAD_TIMESTAMP,
        -- Reason: Update strategy flag - 0 for INSERT, 1 for UPDATE (handled by incremental logic)
        case 
            when sc.LAST_MODIFIED_DATE > sc.REGISTRATION_DATE then 1
            else 0
        end as UPDATE_FLAG
    from source_customer sc
    left join lookup_address la
        on sc.ADDRESS_ID = la.ADDRESS_ID
),

-- Reason: Informatica Filter transformation converted to WHERE clause
filtered_customer as (
    select *
    from transformed_customer
    where 
        -- Reason: Filter condition from FLT_VALID_CUSTOMERS
        CUSTOMER_ID is not null
        and EMAIL != 'noemail@unknown.com'
        and STATUS in ('ACTIVE', 'INACTIVE')
        and REGISTRATION_DATE is not null
)

-- Reason: Final SELECT for dbt model output (replaces Informatica target definition)
select
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    ADDRESS_ID,
    STREET,
    CITY,
    STATE,
    ZIP_CODE,
    COUNTRY,
    STATUS,
    REGISTRATION_DATE,
    LAST_MODIFIED_DATE,
    LOAD_TIMESTAMP
from filtered_customer
