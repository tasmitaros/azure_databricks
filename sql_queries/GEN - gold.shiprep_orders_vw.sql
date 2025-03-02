CREATE OR REPLACE VIEW gold.shiprep_orders_vw AS

SELECT
  c.customer_id,
  c.company_name AS customer_name,
  p.product_id,
  p.product_name,
  e.employee_id,
  e.full_name,
  s.shipper_id,
  s.company_name AS shipper_name,
  o.order_id,
  o.order_date :: DATE,
  o.required_date :: DATE,
  o.ship_date :: DATE,
  o.freight,
  od.unit_price,
  od.quantity,
  od.discount,
  od.unit_price * od.quantity * (1 - od.discount) AS total_price
FROM
  order_details AS od
  LEFT JOIN orders AS o USING (order_id)
  LEFT JOIN products AS p USING (product_id)
  LEFT JOIN customers AS c USING (customer_id)
  LEFT JOIN employees_vw AS e USING (employee_id)
  LEFT JOIN shippers AS s ON s.shipper_id = o.ship_via