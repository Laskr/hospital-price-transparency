with raw_data as (
    select * from {{ source('hospital_raw', 'RAW_BANNER_PHX') }}
)

select
    -- Primary identifiers
    'Banner Health' as hospital_name,
    DESCRIPTION as service_description,
    CODE_1 as billing_code, -- Usually the CPT/HCPCS code

    -- Pricing data
    cast(replace(STANDARD_CHARGE_GROSS, ',', '') as float) as gross_charge,

    -- Selecting specific payers for comparison
    cast(replace(STANDARD_CHARGE_AETNA_AETNA_COMMERCIAL_NEGOTIATED_DOLLAR, ',', '') as float) as aetna_negotiated_rate,
    cast(replace(STANDARD_CHARGE_CIGNA_HEALTHCARE_CIGNA_HEALTHCARE_NEGOTIATED_DOLLAR, ',', '') as float) as cigna_negotiated_rate,
    cast(replace(STANDARD_CHARGE_BLUE_CROSS_AND_BLUE_SHIELD_OF_AZ_BCBS_COMMERCIAL_WORK_COMP_NEGOTIATED_DOLLAR, ',', '') as float) as bcbs_negotiated_rate

from raw_data
where billing_code is not null
  and billing_code != 'code|1'
