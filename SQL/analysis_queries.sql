--To find the Top 10 selling products
select
    product_id,
    count(order_id) as total_sales
from sales_fact
group BY product_id
ORDER BY total_sales desc
limit 10;

--To find the Monthly revenue
select
    year(order_purchase_timestamp) as year_,
    month(order_purchase_timestamp) as month,
    sum(payment_value) AS monthly_rev
from sales_fact
group by year_, month
order by year_, month;

--To find the customer purchase frequency
select
    customer_id,
    count(order_id) AS purchase_count
from sales_fact
group by customer_id
order by purchase_count desc;

--To find the Total sales based on each product
select
    pd.product_category_name,
    count(sf.order_id) as tot_sales
from sales_fact sf
join product_dim pd on sf.product_id = pd.product_id
group by pd.product_category_name
order by tot_sales DESC
limit 10;