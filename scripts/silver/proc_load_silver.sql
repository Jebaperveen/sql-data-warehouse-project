/*
==================================================================================================
store procedure : load silver layer
==================================================================================================
script purpose:
    this procedure perform ETL process to populate the "silver" schemas table from the "bronze" schems
*/


create or alter procedure silver.load_silver as
begin
	declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime;
	begin try 
		set @batch_start_time = getdate();
		set @start_time = getdate();
		print '==========================================================================';
		print'inserting data into : silver.crm_cust_info';
		print '==========================================================================';
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select cst_id,
		cst_key,
		case when trim(cst_firstname)  is null then 'n/a'
				 else trim(cst_firstname) end as cst_firstname,
		 case when trim(cst_lastname)  is null then 'n/a'
				 else trim(cst_lastname) end as cst_lastname,
		case  upper(trim(cst_marital_status)) 
					when 'S' then 'single'
					when 'M' then 'married'
					else 'n/a' 
					end as cst_marital_status,
		case  upper(trim(cst_gndr)) 
					when 'F' then 'female'
					when 'M' then 'male'
					else 'n/a' 
					end as cst_gnder,
		cst_create_date
		from bronze.crm_cust_info;
		set @end_time = getdate();
		print '****** load duration :' +cast(datediff(second, @start_time , @end_time ) as nvarchar ) + 'second';



 

 		print '==========================================================================';
		print 'inserting data into : silver.crm_prd_info';
		print '==========================================================================';
		set @start_time = getdate();
		insert into silver.crm_prd_info(
			prd_id,
			prd_key,
			cat_id,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

		select 
		prd_id,
		replace(substring(prd_key , 1,5),'-','_') as cat_id,
		substring(prd_key ,7 , len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
				when 'M' then 'mountain'
				when 'R' then 'road'
				when 'S' then 'other sales'
				when 'T' then 'touring'
				else 'n/a' end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_end_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		from bronze.crm_prd_info;
		set @end_time = getdate()
		print '****** load duration :' +cast(datediff(second, @start_time , @end_time ) as nvarchar ) + 'second';


		set @start_time = getdate();
		print '==========================================================================';
		print 'inserting data into : silver.crm_sales_details';
		print '==========================================================================';
		insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price) 

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as varchar) as date)
				end as sls_order_dt ,
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
				end as sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
				else cast(cast(sls_due_dt as varchar) as date)
				end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
				else sls_sales end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
				then sls_sales / nullif(sls_quantity,0)
				else sls_price
				end as sls_price
		from bronze.crm_sales_details ;
		set @end_time = getdate();
		print '****** load duration :' +cast(datediff(second, @start_time , @end_time ) as nvarchar ) + 'second';


		set @start_time = getdate();
		print '==========================================================================';
		print 'inserting data into : silver.erp_cust_az12';
		print '==========================================================================';
		insert into silver.erp_cust_az12(
		cid ,
		bdate,
		gen)
		select 
			case when cid like 'NAS%' then substring(cid , 4, len(cid))
				else cid 
			end as cid,
			case when bdate > getdate() then null
				else bdate
			end as bdate,
			case when upper(trim(gen)) in('F' , 'Female')  then 'female'
				when upper(trim(gen)) in('M','Male') then 'male'
			 else 'n\a' end as gen
		from bronze.erp_cust_az12;
		set @end_time = getdate()
		print '****** load duration :' +cast(datediff(second, @start_time , @end_time ) as nvarchar ) + 'second';



		set @start_time = getdate();
		print '==========================================================================';
		print 'inserting data into : silver.erp_loc_a101';
		print '==========================================================================';
		insert into silver.erp_loc_a101(
		cid,
		cntry)
		select  
		replace(cid,'-','') as cid,
		case when trim(cntry) = 'DE' then 'germany'
			when trim(cntry)  in ('US','USA') then 'united states'
			when trim(cntry) = ' ' or cntry is null then 'n/a'
			else trim(cntry)
			end as cntry
		from bronze.erp_loc_a101
		set @end_time = getdate();
		print '****** load duration :' +cast(datediff(second, @start_time , @end_time ) as nvarchar ) + 'second';





		set @start_time = Getdate();
		print '==========================================================================';
		print 'inserting data into : silver.erp_px_cat_g1v2';
		print '==========================================================================';
		insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		set @end_time = getdate();
		print '****** load duration :' +cast(datediff(second, @start_time , @end_time ) as nvarchar ) + 'second';

		set @batch_end_time = getdate()
	end try
	begin catch
		print '========================================================='
		print 'error occured during loading bronze layer'
		print 'error_message' + error_message();
		print 'error message ' + cast(error_number() as nvarchar);
		print '=========================================================';
	end catch
end


