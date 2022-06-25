/*

Pregunta
===========================================================================

Escriba una consulta que retorne la columna `tbl0.c1` y el valor 
correspondiente de la columna `tbl1.c4` para la columna `tbl0.c2`.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;

DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (
    c1 INT,
    c2 INT,
    c3 STRING,
    c4 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data1.csv' INTO TABLE tbl1;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/


DROP TABLE IF EXISTS Resultados0; 
DROP TABLE IF EXISTS Resultados1;

CREATE TABLE Resultados0 AS SELECT c1, c2 FROM tbl0; 
CREATE TABLE Resultados1 AS SELECT c1, c2, Valor FROM tbl1 LATERAL VIEW explode(c4) letras; 
 
INSERT OVERWRITE LOCAL DIRECTORY 'output' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT Resultados0.c1, Resultados.*, Valor FROM Resultados0, Resultados1
WHERE Resultados0.c1 = Resultados1.c1 and Resultados0.c2 = Resultados1.c2;


CREATE TABLE Data_090 AS
SELECT
    c1, c2 key
FROM
    tbl0;

CREATE TABLE Data_091 AS
SELECT
    c1, key, value
FROM
    tbl1
LATERAL VIEW
    EXPLODE(c4) List;

INSERT OVERWRITE LOCAL DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    Dt1.*
FROM
    Data_090 Dt0, Data_091 Dt1
WHERE
    Dt0.c1 = Dt1.c1 AND Dt0.key = Dt1.key;