CREATE OR REPLACE VIEW silver.employees_vw AS

SELECT
    employee_id,
    trim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))) AS full_name,
    title,
    title_of_courtesy,
    birth_date :: DATE,
    hire_date :: DATE,
    address,
    city,
    region,
    postal_code,
    country,
    reports_to
FROM
    employees