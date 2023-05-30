## Unit Test Sample Data

#### Unit Test Data 1
* Insert 3 new documents

Athena query 
```
SELECT * FROM "iceberg"."user" WHERE _id = '647548a6a8579251f116ab6a';
SELECT * FROM "iceberg"."user" WHERE _id = '647548cea8579251f116ab6c';
SELECT * FROM "iceberg"."user" WHERE _id = '64754920a8579251f116ab6e';
```

Each query should return 1 row

#### Unit Test Data 2
* Delete 3 different documents

Athena query
```
SELECT * FROM "iceberg"."user" WHERE _id = '6475485aa8579251f116aab4';
SELECT * FROM "iceberg"."user" WHERE _id = '6475485aa8579251f116aab5';
SELECT * FROM "iceberg"."user" WHERE _id = '6475485aa8579251f116aab6';
```
Each query should return 0 rows

#### Unit Test Data 3
* Update 2 different documents once

```
SELECT * FROM "iceberg"."user" WHERE _id = '6475485aa8579251f116aae0';
SELECT * FROM "iceberg"."user" WHERE _id = '6475485aa8579251f116aae1';
```
_id 6475485aa8579251f116aae0

*first_name*: Bob --> Robert <br>
*email*: bploughwright18@163.com --> bploughwright18@gmail.com

_id 6475485aa8579251f116aae1
*gender*: Male --> Female
*card.card_type*: diners-club-enroute --> master_card

#### Unit Test 4
* Insert 2 new doucuments
* Delete 2 different documents
* Update 2 different documents once
* Update 1 document 3 different times
