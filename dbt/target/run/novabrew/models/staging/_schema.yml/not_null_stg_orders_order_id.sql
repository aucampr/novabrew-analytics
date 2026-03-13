
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_orders`
where order_id is null



  
  
      
    ) dbt_internal_test