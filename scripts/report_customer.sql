select
year(order_date) as order_year,
count(distinct customer_key) as total_customers,
sum(sales_amount ) as total_sales,
sum(quantity) as total_qty
from dbo.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date);


select
month(order_date) as order_year,
count(distinct customer_key) as total_customers,
sum(sales_amount ) as total_sales,
sum(quantity) as total_qty
from dbo.fact_sales
where order_date is not null
group by month(order_date)
order by month(order_date);

select
year(order_date) as order_year,
month(order_date) as order_month,
count(distinct customer_key) as total_customers,
sum(sales_amount ) as total_sales,
sum(quantity) as total_qty
from dbo.fact_sales
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date) ,month(order_date);


select
datetrunc( month ,order_date) as order_month,
count(distinct customer_key) as total_customers,
sum(sales_amount ) as total_sales,
sum(quantity) as total_qty
from dbo.fact_sales
where order_date is not null
group by datetrunc( month ,order_date)
order by datetrunc( month ,order_date);


select 
format(order_date , 'yyyy-MMM') as order_date,
count(distinct customer_key) as total_customers,
sum(sales_amount ) as total_sales,
sum(quantity) as total_qty
from dbo.fact_sales
where order_date is not null
group by format(order_date , 'yyyy-MMM') 
order by format(order_date , 'yyyy-MMM') ;

-- calculate total sales per month
-- and running totaal of sales over time
select order_date,
total_sales,
avg_price,
sum(total_sales) over(order by order_date) as running_total_sales,
avg(avg_price) over(order by order_date) as moving_average
from
(
select 
datetrunc( month ,order_date ) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from dbo.fact_sales
where order_date is not null
group by datetrunc(month ,order_date)
) t;

-- Analyze yearly performance of products by comparing their sales 
-- to both the average sales performance of product and previous year's sales.


with yearly_product_sales as
(
select 
year(f.order_date) as order_year,
p.product_name as product_name,
sum(f.sales_amount) as current_sales
from dbo.fact_sales f
left join dbo.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by  year(f.order_date), p.product_name)

select 
order_year,
product_name,
current_sales,
avg(current_sales) over(partition by product_name) as avg_sales,
current_sales -  avg(current_sales) over(partition by product_name) as diff_avg,
case when current_sales -  avg(current_sales) over(partition by product_name) > 0 then 'above avg'
     when current_sales -  avg(current_sales) over(partition by product_name) < 0 then 'below avg'
     else 'avg' end as avg_change,
lag(current_sales) over(partition by product_name order by order_year) as previous_year_sales,
current_sales -  lag(current_sales) over(partition by product_name order by order_year) as diff_py,
case when current_sales -  lag(current_sales) over(partition by product_name order by order_year)> 0 then 'increase'
     when current_sales -  lag(current_sales) over(partition by product_name order by order_year) < 0 then 'decrease'
     else 'no change' end as py_change
from yearly_product_sales 
order by product_name , order_year



-- which category contribute the most to overall sales?
with category_sales as
(
select 
p.category as category,
sum(f.sales_amount) as total_sales
from dbo.fact_sales f
left join dbo.dim_products p
on f.product_key = p.product_key
group by p.category
) 
select 
category ,
total_sales,
sum(total_sales) over() overall_sales,
concat(round((cast(total_sales  as float)/ sum(total_sales) over())*100,2) , ' %') as percentage_total
from category_sales
order by total_sales desc



-- segment the product into cost ranges and
-- count how many product fall into each category
with product_segment as
(
select 
product_key,
product_name,
cost,
case when cost < 100 then 'below 100'
     when cost between 100 and 800 then '100 - 800'
     when cost between 800 and 1500 then '800 -  1500'
     else 'above 1500'
     end as cost_range
from dbo.dim_products
)
select 
cost_range,
count(product_key) as total_products
from product_segment
group by cost_range
order by total_products desc


/* group the customers into three segments based on their spending behaviour:
    VIP : customers with atleast 12 months of history and spending more than 5000.
    regular: customers with atleast 12 months of history and spending 5000 or less.
    new : customers with a lifespan less then 12 months.
and find total number of customer by each group */

with customer_spending as 
(
select 
c.customer_key,
sum(f.sales_amount) as total_spending,
min(f.order_date) as first_order,
max(f.order_date) as last_order,
datediff(month ,min(f.order_date),max(f.order_date) ) as lifespan
from dbo.fact_sales f
left join dbo.dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key
)

select 
customer_segment,
count(customer_key) as total_customer
from(
    select 
    customer_key,
    case when lifespan >= 12 and total_spending > 5000 then 'Vip'
         when lifespan >=12 and total_spending <= 5000 then 'Regular'
         else 'New' 
         end as customer_segment
    from customer_spending)t
group by customer_segment;
    


/* 
=======================================================================================================
    customer report 
=======================================================================================================
purpose : 
        this report consolidates key customer metrices and behaviours

highlights:
        1. Gather essential fields such as names, ages and transaction details.
        2. segments customers into categories ( VIP, Regular , new) and age groups
        3. aggregates customer level metrices
            total orders
            total sales
            total products
            lifespan in months
        4. calculates valuable KPIs
            recency (months since last order)
            average order value
            average monthly spend
=========================================================================================================
*/
create view dbo.report_customer as 

with base_query AS(
    select 
    f.order_number,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    concat(c.first_name , ' ' , c.last_name ) as customer_name,
    datediff(year ,c.birthdate, getdate()) as age
    from
    dbo.fact_sales f
    left join 
    dbo.dim_customers c
    on c.customer_key = f.customer_key
    where f.order_date is not null
)
,
 customer_agg as ( 
    select
    customer_key,
    customer_number,
    customer_name,
    age,
    count(distinct order_number) as total_orders,
    sum(sales_amount) as total_sales,
    sum(quantity) as total_qty,
    count(distinct product_key) as total_products,
    max(order_date) as last_order,
    datediff(month , min(order_date) , max(order_date)) as lifespan
    from base_query
    group by customer_key,
            customer_number,
            customer_name,
            age
        )
   select 
   customer_key,
   customer_number,
   customer_name,
   age,
   case when  age < 20 then 'under 20'
        when age between 20 and 29 then '20-29'
        when age between 30 and 39 then '30-39'
        when age between 40 and 49 then '40-49'
        else 'above 50' end as age_group,


   case when lifespan >= 12 and total_sales > 5000 then 'Vip'
         when lifespan >=12 and total_sales <= 5000 then 'Regular'
         else 'New' 
         end as customer_segment,
    last_order ,
   datediff(month , last_order , getdate()) as recency,
   total_orders,
   total_sales,
   total_qty,
   total_products,
   lifespan,
   case when total_orders = 0 then 0
        else total_sales/ total_orders end as avg_order_value,
   case when lifespan = 0 then 0
        else total_sales / lifespan end as avg_monthly_spend  
 from customer_agg;


/*
================================================================================================
prdouct report
================================================================================================
purpose :
    this report consolidates key product matrices and behaviours.

highlights:
    1. gather essential fields such as product name , category , subcategory  and cost.
    2. segments product by revenue to identity high performers, mid range or low performers.
    3. aggregate product level metrices:
        total orders
        total sales
        total quantity sold
        lifespan 
    4. calculates value KPIs :
        recency month since last sale
        average order revnue
        average monthly revnue
===================================================================================================
*/

create view dbo.report_products as
with base_query as (
    select 
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    from dbo.fact_sales f
    left join dbo.dim_products p 
    on p.product_key = f.product_key
    where order_date is not null
    )
,
product_agg as(
select 
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        datediff(month , min(order_date) , max(order_date)) as lifespan,
        max(order_date) as last_sale_date,
        count(distinct order_number) as total_orders,
        count(distinct customer_key) as total_customers,
        sum(sales_amount) as total_sales,
        sum(quantity) as total_quantity,
        round(avg(cast(sales_amount as float)/nullif(quantity,0)),1) as avg_selling_price
from base_query
group by  
        product_key,
        product_name,
        category,
        subcategory,
        cost
)
select 
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        last_sale_date,
        datediff(month , last_sale_date , getdate()) as recency_in_months,
        case when total_sales > 5000 then 'high performer'
             when total_sales  > = 10000 then 'mid range'
             else 'low performer' end as product_segment,
        lifespan,
        total_orders,
        total_sales,
        total_quantity,
        total_customers,
        avg_selling_price,
        case when total_orders = 0 then 0 
            else total_sales/ total_orders end as avg_order_revenue,
        case when lifespan = 0 then total_sales
            else total_sales/ lifespan end as avg_monthly_revenue
    from product_agg



