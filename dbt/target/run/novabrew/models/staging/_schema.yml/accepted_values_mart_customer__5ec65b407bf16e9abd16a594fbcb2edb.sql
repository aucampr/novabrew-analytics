
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        r_score as value_field,
        count(*) as n_records

    from `novabrew-analytics`.`novabrew_dev_novabrew_marts`.`mart_customer_segments`
    group by r_score

)

select *
from all_values
where value_field not in (
    '1','2','3','4','None'
)



  
  
      
    ) dbt_internal_test