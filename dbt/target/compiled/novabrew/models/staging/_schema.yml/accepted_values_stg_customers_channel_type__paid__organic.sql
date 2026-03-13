
    
    

with all_values as (

    select
        channel_type as value_field,
        count(*) as n_records

    from `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_customers`
    group by channel_type

)

select *
from all_values
where value_field not in (
    'paid','organic'
)


