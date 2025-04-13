# MongoDB Streams AWS Glue

<img width="85" alt="map-user" src="https://img.shields.io/badge/views-1617-green"> <img width="125" alt="map-user" src="https://img.shields.io/badge/unique visits-235-green">

This repository has a solution for a 1 time copy of data from a MongoDB collection into an Apache Iceberg table in S3 and a solution to use the MongoDB change stream to keep the iceberg copy of the data up to date.

The architecture below depicts the solution

<br>

<img width="1000" alt="map-user" src="https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/0_Architecture/mongoDB_glue_iceberg_architecture.png">

The repository is broken down into several sections. Each section has its own read me that will explains its components

* [1_Sample_MongoDB_Data](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/tree/main/1_Sample_MongoDB_Data) steps 1 and 2 in architecture diagram.

* [2_Glue_Iceberg_Initial_Load](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/tree/main/2_Glue_Iceberg_Initial_Load) step 3 in the architecture diagram

* [3_Sample_MongoDB_Change_Stream_Data](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/tree/main/2_Glue_Iceberg_Initial_Load) steps 4 and 5 in the architecture diagram

* [4_Glue_Iceberg_Change_Stream](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/tree/main/2_Glue_Iceberg_Initial_Load) step 6 in the architecture diagram
