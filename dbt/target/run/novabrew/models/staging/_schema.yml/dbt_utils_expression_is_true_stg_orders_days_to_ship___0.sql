
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_orders`

where not(days_to_ship >= 0)


  
  
      
    ) dbt_internal_test