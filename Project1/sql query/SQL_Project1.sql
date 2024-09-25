use challenge1;
show tables;

select * from members;
select * from menu;
select * from sales;

#1. What is the total amount each customer spent at the restaurant?
select s.customer_id, sum(price) as Total
from sales s
join menu m on s.product_id = m.product_id
group by customer_id;

#2. How many days has each customer visited the restaurant?
select customer_id, count(distinct order_date) as Total_Visit
from sales
group by customer_id;

#3. What was the first item from the menu purchased by each customer?
select s.customer_id, s.order_date, m.product_name
from sales s
join menu m on s.product_id = m.product_id
where s.order_date = (
	select min(order_date)
    from sales s2
    where s2.customer_id = s.customer_id);

#4. What is the most purchased item on the menu and how many times was it purchased by all customers?
SELECT m.product_name, COUNT(s.product_id) AS total_purchases
FROM sales s
JOIN menu m ON s.product_id = m.product_id
GROUP BY m.product_name
ORDER BY total_purchases DESC;

#5. Which item was the most popular for each customer?
#Approach 1 using Join and Aggregate 
SELECT s.customer_id, m.product_name, COUNT(s.product_id) AS total_purchases
FROM sales s
JOIN menu m ON s.product_id = m.product_id
GROUP BY s.customer_id, m.product_name
HAVING 
    COUNT(s.product_id) = (
        SELECT MAX(purchase_count)
        FROM (
            SELECT COUNT(s2.product_id) AS purchase_count
            FROM sales s2
            WHERE s2.customer_id = s.customer_id
            GROUP BY s2.product_id
        ) AS subquery
    );

#Approach two using windows function (rank())
SELECT customer_id, product_name, total_purchases
FROM (
    SELECT s.customer_id, m.product_name, COUNT(s.product_id) AS total_purchases,
        RANK() OVER (PARTITION BY s.customer_id ORDER BY COUNT(s.product_id) DESC) AS rank1
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    GROUP BY s.customer_id, m.product_name
) ranked
WHERE rank1 = 1;

#6. Which item was purchased first by the customer after they became a member?
SELECT s.customer_id, m.product_name, s.order_date
FROM sales s
JOIN menu m ON s.product_id = m.product_id
JOIN members mb ON s.customer_id = mb.customer_id
WHERE s.order_date = (
        SELECT MIN(s2.order_date)
        FROM sales s2
        WHERE s2.customer_id = s.customer_id AND s2.order_date >= mb.join_date
    );


#7. Which item was purchased just before the customer became a member?
SELECT s.customer_id, m.product_name, s.order_date
FROM sales s
JOIN menu m ON s.product_id = m.product_id
JOIN members mb ON s.customer_id = mb.customer_id
WHERE s.order_date = (
        SELECT MIN(s2.order_date)
        FROM sales s2
        WHERE s2.customer_id = s.customer_id AND s2.order_date >= mb.join_date
    );
    
#8.What is the total items and amount spent for each member before they became a member?
SELECT mb.customer_id, COUNT(s.product_id) AS total_items, SUM(m.price) AS total_amount_spent
FROM members mb
LEFT JOIN sales s ON mb.customer_id = s.customer_id
JOIN menu m ON s.product_id = m.product_id
WHERE s.order_date < mb.join_date
GROUP BY mb.customer_id;

#9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
SELECT s.customer_id,
    SUM(
        CASE 
            WHEN m.product_name = 'sushi' THEN m.price * 20  -- 2x multiplier for sushi
            ELSE m.price * 10
        END
    ) AS total_points
FROM sales s
JOIN menu m ON s.product_id = m.product_id
GROUP BY s.customer_id
ORDER BY s.customer_id;






