----- cleaning our data ----

--- ALL RECORDS 541909 ----
--- CUSTOMER WITH OUT ID# 134466 ---
--- CUSTOMER WITH  ID# 406376 ---

;WITH o_retail as
(
select *
from [dbo].[online_retai2]
WHERE CustomerID != 0 
)
 , quant_unit as(
 
 ----- updated records 394839 -----
	select *
	from o_retail
	where Quantity > 0 and UnitPrice > 0 
)
, dublicate_check as 
(
 ------ check dublicate -----
select *
        , ROW_NUMBER() over (partition by InvoiceNo, StockCode, Quantity order by invoicedate) dublicate_val
from quant_unit
)
-----  this is the UNIQUE values -----

------ Data Records 389669 ----

----- We will create a Temp table for the clean dataset ---- We do not want to use CTEs more than that -----

select *
into #online_retail 
from dublicate_check
where dublicate_val = 1

 ------ Clean data ----
 ----- We can start our Cohort Analysis ----
select *
from #online_retail

----- we need Unique ID Which will be (customer ID ) ---
----- WE need to know the initial start date ( first invoice ) ---
-----  Revenue  ----

select CustomerID, 
	min(invoicedate) first_purchase,
	DATEFROMPARTS (year(min(invoicedate)), month(min(invoicedate)),1) year_month
	into #cohort_analysis 
from #online_retail
group by CustomerID

select*
from #cohort_analysis

----CREATE COHORT INDEX -----
select oo.*,
cohort_index = year_diff * 12 + month_diff + 1
into #cohort_ret
from (
	select online.*,
		   year_diff = invoice_year - cohort_year,
		   month_diff = invoice_month - cohort_month
	from (
		SELECT o.*,
			   c.year_month,
			   year(o.InvoiceDate)invoice_year,
			   month(o.InvoiceDate)invoice_month,
			   year(c.year_month)cohort_year,
			   month(c.year_month)cohort_month
		FROM #online_retail o
		LEFT JOIN #cohort_analysis c
		   ON o.CustomerID = c.CustomerID
		   ) online
		   ) oo

select *
from #cohort_ret

----- extract some data for tableau ---

select distinct CustomerID, cohort_index,year_month
from #cohort_ret

---Pivot Data to see the cohort table
select 	*
into #cohort_pivot
from(
select distinct CustomerID, cohort_index, year_month from #cohort_ret
)tbl
pivot(
	Count(CustomerID)
	for Cohort_Index In 
		(
		[1], 
        [2], 
        [3], 
        [4], 
        [5], 
        [6], 
        [7],
		[8], 
        [9], 
        [10], 
        [11], 
        [12],
		[13])

)as pivot_table

select *
from #cohort_pivot
order by year_month

select year_month ,
	(1.0 * [1]/[1] * 100) as [1], 
    1.0 * [2]/[1] * 100 as [2], 
    1.0 * [3]/[1] * 100 as [3],  
    1.0 * [4]/[1] * 100 as [4],  
    1.0 * [5]/[1] * 100 as [5], 
    1.0 * [6]/[1] * 100 as [6], 
    1.0 * [7]/[1] * 100 as [7], 
	1.0 * [8]/[1] * 100 as [8], 
    1.0 * [9]/[1] * 100 as [9], 
    1.0 * [10]/[1] * 100 as [10],   
    1.0 * [11]/[1] * 100 as [11],  
    1.0 * [12]/[1] * 100 as [12],  
	1.0 * [13]/[1] * 100 as [13]
from #cohort_pivot
order by year_month

