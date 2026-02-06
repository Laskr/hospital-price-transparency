with raw_data as (
    select * from {{ source('hospital_raw', 'RAW_MAYO_AZ') }}
)

select
    'Mayo Clinic' as hospital_name,
    COL_0 as service_description,
    COL_1 as billing_code,

    -- Column 16 is Gross Charge in the CMS v2.0 CSV spec
    cast(replace(COL_16, ',', '') as float) as gross_charge,

    -- Payers are in rows not columns
    COL_22 as payer_name,
    cast(replace(COL_25, ',', '') as float) as negotiated_rate

from raw_data
where billing_code is not null
  and billing_code != 'code|1'
  and (
      payer_name ilike '%Aetna%'
      or payer_name ilike '%Cigna%'
      or payer_name ilike '%Blue Cross%'
  )
