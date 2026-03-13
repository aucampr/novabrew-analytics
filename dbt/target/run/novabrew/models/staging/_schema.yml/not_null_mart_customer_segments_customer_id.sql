
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from `novabrew-analytics`.`novabrew_dev_novabrew_marts`.`mart_customer_segments`
where customer_id is null



  
  
      
    ) dbt_internal_test