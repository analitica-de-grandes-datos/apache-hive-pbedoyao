/*

Pregunta
===========================================================================

Escriba una consulta que compute la cantidad de registros por letra de la 
columna 2 y clave de la columna 3; esto es, por ejemplo, la cantidad de 
registros en tienen la letra `a` en la columna 2 y la clave `aaa` en la 
columna 3 es:

    a    aaa    5

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.
*/

/*
    >>> Escriba su respuesta a partir de este punto <<<

*/

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;

DROP TABLE IF EXISTS Resultado;
CREATE TABLE Resultado AS SELECT letra, letras, count(*) AS cantidad FROM t0
LATERAL VIEW explode(c2) data AS letra
LATERAL VIEW explode(c3) data1 AS letras, valor
GROUP BY letra, letras;
 
INSERT OVERWRITE LOCAL DIRECTORY 'output' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT * FROM Resultado ;