SELECT
    D1.col1 AS A,
    D1.col2 AS B,
    D2.col2 AS G,
    D2.col3 AS H,
    D3.col3 AS M,
    D3.col4 AS N,
    D4.col4 AS S,
    D4.col5 AS T
FROM
    fact_table,
    LATERAL TABLE (dimension_table1(f_proctime)) AS D1,
    LATERAL TABLE (dimension_table2(f_proctime)) AS D2,
    LATERAL TABLE (dimension_table3(f_proctime)) AS D3,
    LATERAL TABLE (dimension_table4(f_proctime)) AS D4
WHERE
    fact_table.dim1     = D1.id
    AND fact_table.dim2 = D2.id
    AND fact_table.dim3 = D3.id
    AND fact_table.dim4 = D4.id
