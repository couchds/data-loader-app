CREATE TABLE IF NOT EXISTS sales_table (
    order_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    total_amount DECIMAL(10,2),
    purchase_date DATE
);