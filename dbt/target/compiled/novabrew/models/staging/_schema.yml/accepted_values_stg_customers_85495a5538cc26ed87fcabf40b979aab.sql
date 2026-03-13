
    
    

with all_values as (

    select
        acquisition_channel as value_field,
        count(*) as n_records

    from `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_customers`
    group by acquisition_channel

)

select *
from all_values
where value_field not in (
    'paid_search','paid_social','organic_search','organic_social','email','direct','referral','unknown'
)


