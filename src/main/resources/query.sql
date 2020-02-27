SELECT
    D1.col1 AS A,
    D1.col2 AS B,
    D1.col3 AS C,
    D1.col4 AS D,
    D1.col5 AS E,
    D2.col1 AS F,
    D2.col2 AS G,
    D2.col3 AS H,
    D2.col4 AS I,
    D2.col5 AS J,
    D3.col1 AS K,
    D3.col2 AS L,
    D3.col3 AS M,
    D3.col4 AS N,
    D3.col5 AS O,
    D4.col1 AS P,
    D4.col2 AS Q,
    D4.col3 AS R,
    D4.col4 AS S,
    D4.col5 AS T,
    D5.col1 AS U,
    D5.col2 AS V,
    D5.col3 AS W,
    D5.col4 AS X,
    D5.col5 AS Y
FROM
    fact_table,
    LATERAL TABLE (dimension_table1(f_proctime)) AS D1,
    LATERAL TABLE (dimension_table2(f_proctime)) AS D2,
    LATERAL TABLE (dimension_table3(f_proctime)) AS D3,
    LATERAL TABLE (dimension_table4(f_proctime)) AS D4,
    LATERAL TABLE (dimension_table5(f_proctime)) AS D5
WHERE
    fact_table.dim1     = D1.id
    AND fact_table.dim2 = D2.id
    AND fact_table.dim3 = D3.id
    AND fact_table.dim4 = D4.id
    AND fact_table.dim5 = D5.id
