
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `novabrew-analytics`.`novabrew_dev_novabrew_marts`.`mart_customer_segments`

where not(total_revenue >= 0)


  
  
      
    ) dbt_internal_test