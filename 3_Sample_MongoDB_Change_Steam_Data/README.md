## Unit Test Sample Data

#### Unit Test Data 1
* Insert 3 new documents

Athena query 
```
SELECT * FROM "iceberg"."user" WHERE _id = '64753f81a1309e5efb435f7a';
SELECT * FROM "iceberg"."user" WHERE _id = '64753f90a1309e5efb435f7c';
SELECT * FROM "iceberg"."user" WHERE _id = '64753f9da1309e5efb435f7e';
```

Each query should return 1 row

#### Unit Test Data 2
* Delete 3 different documents

Athena query
```
SELECT * FROM "iceberg"."user" WHERE _id = '64753f38a1309e5efb435ecb';
SELECT * FROM "iceberg"."user" WHERE _id = '64753f38a1309e5efb435ecc';
SELECT * FROM "iceberg"."user" WHERE _id = '64753f38a1309e5efb435ecd';
```
Each query should return 0 rows

#### Unit Test Data 3
* Update 3 different documents once

#### Unit Test 4
* Insert 2 new doucuments
* Delete 2 different documents
* Update 2 different documents once
* Update 1 document 3 different times
