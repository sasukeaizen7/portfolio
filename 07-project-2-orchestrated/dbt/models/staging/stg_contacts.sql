-- One row per call. `duration` is dropped on purpose: a call's length is only known after the call,
-- and it almost determines the outcome (long calls = sales). Using it to predict conversion is target leakage.
select
    row_number                     as contact_id,
    contact_month,
    year,
    age,
    age_band,
    job,
    marital,
    education,
    contact                        as channel,         -- cellular / telephone
    day_of_week,
    campaign                       as contacts_this_campaign,
    previously_contacted,
    days_since_previous,
    previous                       as contacts_before_this_campaign,
    poutcome                       as previous_outcome,
    euribor3m                      as euribor_3m,
    emp_var_rate                   as employment_variation_rate,
    converted
from {{ source('raw', 'marketing_contacts') }}
