SELECT
*
from


(select 


StoreId,
BusinessDate,
Sales,
rank() over (partition by BusinessDate order by Sales) as Rank

 from `sales.sales_data_complete` 
 where Sales > 1000 
 )
 where Rank =1
 order by Sales DESC