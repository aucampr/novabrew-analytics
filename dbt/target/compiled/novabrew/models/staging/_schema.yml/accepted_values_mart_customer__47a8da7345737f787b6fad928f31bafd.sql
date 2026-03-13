
    
    

with all_values as (

    select
        segment as value_field,
        count(*) as n_records

    from `novabrew-analytics`.`novabrew_dev_novabrew_marts`.`mart_customer_segments`
    group by segment

)

select *
from all_values
where value_field not in (
    'Champion','Loyal','Promising','Needs Attention','At Risk','Lost','Occasional','Non-Buyer'
)


