
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_orders`
    group by status

)

select *
from all_values
where value_field not in (
    'delivered','cancelled','refunded'
)


