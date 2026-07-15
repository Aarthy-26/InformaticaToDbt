-- =============================================
-- Author: Ascendion AAVA
-- Created on:
-- Description: Customer load mapping with transformations and lookup
-- =============================================

-- =========================================================
-- CONVERSION LOG
-- =========================================================
-- Input Type : Informatica Mapping
-- Target Platform : dbt (SQL Model)
-- Conversion Approach :
-- - Translated Informatica mapping logic (transformations, expressions, lookups) into dbt SQL model constructs
-- - Replaced Informatica built-in functions (IIF, DECODE, NVL, TRUNC, INSTR, SUBSTR, SYSDATE, etc.) with dbt/Jinja + warehouse SQL equivalents
-- - Converted Informatica sources and targets into dbt {{ source() }} / {{ ref() }} references
-- - Replaced Update Strategy transformations with dbt incremental model logic (is_incremental(), unique_key)
-- - Converted Rank / Sequence transformations into window functions or surrogate key macros (e.g. dbt_utils.generate_surrogate_key)
-- - Converted Slowly Changing Dimension logic into dbt snapshot constructs where applicable
-- - Added inline migration comments where source logic required dbt-specific restructuring
-- - Ensured compatibility with dbt Core / dbt Cloud on the target warehouse
-- Major Risks / Checks :
-- - Validate datatype mappings between Informatica source/target ports and warehouse column types
-- - Validate NULL semantics and string concatenation behavior
-- - Validate incremental/merge logic against the original Update Strategy behavior
-- - Validate that the ref()/source() dependency graph resolves correctly (no missing or broken sources)
-- - Validate materialization choice (view / table / incremental) matches the original load pattern
-- - Validate companion schema.yml tests reflect the original Informatica data quality checks
-- =========================================================

{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    tags=['customer', 'dimension']
) }}

-- Reason: Source qualifier logic converted to CTE with dbt source reference
with sq_customer as (
    select
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTRY,
        CUSTOMER_TYPE,
        CREDIT_LIMIT,
        ACCOUNT_BALANCE,
        REGISTRATION_DATE,
        LAST_PURCHASE_DATE,
        STATUS,
        CREATED_DATE,
        UPDATED_DATE
    from {{ source('staging', 'customer_raw') }}
    {% if is_incremental() %}
    -- Reason: Incremental filter replaces Informatica session-level filter for delta loads
    where UPDATED_DATE > (select max(UPDATED_DATE) from {{ this }})
    {% endif %}
),

-- Reason: Lookup transformation converted to LEFT JOIN with dimension table reference
lkp_country as (
    select
        COUNTRY_CODE,
        COUNTRY_NAME,
        REGION,
        CURRENCY_CODE
    from {{ ref('dim_country') }}
),

-- Reason: Expression transformation logic converted to SELECT with CASE statements
exp_transform as (
    select
        sq.CUSTOMER_ID,
        -- Reason: IIF converted to CASE WHEN for standard SQL compatibility
        case 
            when sq.FIRST_NAME is null or trim(sq.FIRST_NAME) = '' 
            then 'UNKNOWN' 
            else upper(trim(sq.FIRST_NAME)) 
        end as FIRST_NAME_CLEAN,
        case 
            when sq.LAST_NAME is null or trim(sq.LAST_NAME) = '' 
            then 'UNKNOWN' 
            else upper(trim(sq.LAST_NAME)) 
        end as LAST_NAME_CLEAN,
        -- Reason: String concatenation using standard SQL || operator
        upper(trim(coalesce(sq.FIRST_NAME, ''))) || ' ' || upper(trim(coalesce(sq.LAST_NAME, ''))) as FULL_NAME,
        -- Reason: NVL converted to COALESCE for NULL handling
        coalesce(lower(trim(sq.EMAIL)), 'no-email@unknown.com') as EMAIL_CLEAN,
        -- Reason: SUBSTR and INSTR converted to SUBSTRING and POSITION
        case 
            when position('@' in sq.EMAIL) > 0 
            then substring(sq.EMAIL, position('@' in sq.EMAIL) + 1, length(sq.EMAIL))
            else 'unknown.com'
        end as EMAIL_DOMAIN,
        coalesce(sq.PHONE, 'UNKNOWN') as PHONE,
        coalesce(sq.ADDRESS, 'UNKNOWN') as ADDRESS,
        coalesce(sq.CITY, 'UNKNOWN') as CITY,
        coalesce(sq.STATE, 'UNKNOWN') as STATE,
        coalesce(sq.ZIP_CODE, '00000') as ZIP_CODE,
        coalesce(sq.COUNTRY, 'US') as COUNTRY_CODE,
        -- Reason: Lookup join result with NULL handling
        coalesce(lkp.COUNTRY_NAME, 'Unknown Country') as COUNTRY_NAME,
        coalesce(lkp.REGION, 'Unknown Region') as REGION,
        coalesce(lkp.CURRENCY_CODE, 'USD') as CURRENCY_CODE,
        -- Reason: DECODE converted to CASE statement
        case sq.CUSTOMER_TYPE
            when 'R' then 'RETAIL'
            when 'W' then 'WHOLESALE'
            when 'C' then 'CORPORATE'
            else 'OTHER'
        end as CUSTOMER_TYPE_DESC,
        -- Reason: Numeric transformation with NULL handling
        coalesce(sq.CREDIT_LIMIT, 0) as CREDIT_LIMIT,
        coalesce(sq.ACCOUNT_BALANCE, 0) as ACCOUNT_BALANCE,
        -- Reason: Credit utilization calculation with divide-by-zero protection
        case 
            when coalesce(sq.CREDIT_LIMIT, 0) = 0 then 0
            else round((coalesce(sq.ACCOUNT_BALANCE, 0) / sq.CREDIT_LIMIT) * 100, 2)
        end as CREDIT_UTILIZATION_PCT,
        -- Reason: Risk category based on business rules
        case 
            when coalesce(sq.CREDIT_LIMIT, 0) = 0 then 'NO_CREDIT'
            when (coalesce(sq.ACCOUNT_BALANCE, 0) / sq.CREDIT_LIMIT) > 0.9 then 'HIGH_RISK'
            when (coalesce(sq.ACCOUNT_BALANCE, 0) / sq.CREDIT_LIMIT) > 0.7 then 'MEDIUM_RISK'
            else 'LOW_RISK'
        end as RISK_CATEGORY,
        sq.REGISTRATION_DATE,
        sq.LAST_PURCHASE_DATE,
        -- Reason: TRUNC converted to DATE_TRUNC for date truncation
        date_trunc('day', sq.REGISTRATION_DATE) as REGISTRATION_DATE_KEY,
        -- Reason: Date difference calculation using DATEDIFF
        case 
            when sq.LAST_PURCHASE_DATE is not null 
            then datediff('day', sq.LAST_PURCHASE_DATE, current_date)
            else null
        end as DAYS_SINCE_LAST_PURCHASE,
        -- Reason: Customer lifecycle status logic
        case 
            when sq.STATUS = 'A' and sq.LAST_PURCHASE_DATE >= dateadd('day', -90, current_date) then 'ACTIVE'
            when sq.STATUS = 'A' and sq.LAST_PURCHASE_DATE < dateadd('day', -90, current_date) then 'INACTIVE'
            when sq.STATUS = 'S' then 'SUSPENDED'
            when sq.STATUS = 'C' then 'CLOSED'
            else 'UNKNOWN'
        end as LIFECYCLE_STATUS,
        -- Reason: Customer tenure calculation
        datediff('month', sq.REGISTRATION_DATE, current_date) as CUSTOMER_TENURE_MONTHS,
        -- Reason: SYSDATE converted to CURRENT_TIMESTAMP
        sq.CREATED_DATE,
        sq.UPDATED_DATE,
        current_timestamp as DW_LOAD_TIMESTAMP,
        -- Reason: Surrogate key generation using dbt_utils macro
        {{ dbt_utils.generate_surrogate_key(['sq.CUSTOMER_ID', 'sq.UPDATED_DATE']) }} as DW_RECORD_KEY
    from sq_customer sq
    left join lkp_country lkp
        on sq.COUNTRY = lkp.COUNTRY_CODE
)

-- Reason: Final SELECT represents the target output; dbt materialization handles INSERT/UPDATE
select
    CUSTOMER_ID,
    FIRST_NAME_CLEAN as FIRST_NAME,
    LAST_NAME_CLEAN as LAST_NAME,
    FULL_NAME,
    EMAIL_CLEAN as EMAIL,
    EMAIL_DOMAIN,
    PHONE,
    ADDRESS,
    CITY,
    STATE,
    ZIP_CODE,
    COUNTRY_CODE,
    COUNTRY_NAME,
    REGION,
    CURRENCY_CODE,
    CUSTOMER_TYPE_DESC as CUSTOMER_TYPE,
    CREDIT_LIMIT,
    ACCOUNT_BALANCE,
    CREDIT_UTILIZATION_PCT,
    RISK_CATEGORY,
    REGISTRATION_DATE,
    LAST_PURCHASE_DATE,
    REGISTRATION_DATE_KEY,
    DAYS_SINCE_LAST_PURCHASE,
    LIFECYCLE_STATUS,
    CUSTOMER_TENURE_MONTHS,
    CREATED_DATE,
    UPDATED_DATE,
    DW_LOAD_TIMESTAMP,
    DW_RECORD_KEY
from exp_transform
