# Final_Project - Data Pipeline - Team 3

## Table of Contents

- [Project Overview](#overview)
- [Project Architecture](#architecture)
- [Project Components](#components)
- [Prequesties](#prequesties)
- [Usage](#usage)
- [More Documents](#doc)
- [Contributors](#contributors
***


<a id='overview'></a>
## 1. Project Overview
Rapid development and expansion of the business segment inside company A have led to a fiercer requirement for having an ETL data pipeline that can be automated, robust, and capable of handling/preprocessing a large volume of data. 
Acknowledging those requirements, this project aims to build a sample data pipeline for a Marketing department that satisfies all the above requirements and is capable of returning results that match the specific demand of this business unit ([more](https://docs.google.com/spreadsheets/d/10420ZdVqh9tX8HHfqCQbzv6Rer4oypdxQ7RoEpgTo7A/edit?usp=sharing)). 

The project overall can be separated into three main parts Collect data from local database and save to datalake, Transformation data, and Load the processed data to the database.
Overall, the processes will utilize Spark's distributed processing capabilities as the main pipeline, MongoDB as the main Storage Database for its simplicity and support for distributed, data replication, and Airflow - scheduler for simplicity. 


<a id='architecture'></a>
## 2. Architecture
![FinalProjectDesign-Architecture_Design](https://user-images.githubusercontent.com/78945505/155892212-ff5400b1-4043-4e35-bb2a-d5bfaee6da3e.jpeg)

The system will be scheduled to run on a daily basis, daily logs produced by other applications will be stored and picked up by spark and save to the data lake. The process continues with transforming data to the required format. The transformed data will then be put in the data warehouse. 

There are two main approaches for pipeline designing, the first one will prioritize the transformation and processing data on Spark. In contrast, the second method will favor the use of MongoDB as the data deduplication and sanitizer. Both methods contain many pros and cons. By following the former approach, the project is expected to have a higher data transfer between spark- Mongo in exchange for a faster and less stressful database activity (since all processing has been done on Spark).

To simplify and minimize the effort in setting up systems, the project used local file system as storage for input files and datalake.


<a id='components'></a>
## 3. Specific Components
### 3.1 Airflow: Scheduler
Responsible for the daily dag run and manage all the. ..

#### Main Tasks
+ 
#### Consideration
+

### 3.2 Spark: Main framework for data pipeline 
This project use Spark as the framework for handling the ETL 
#### Main Tasks:
+ Read the input data from the specific folder
+ Save raw data in parquet format in the datalake
+ Transformation for the data into specific format - calculation
+ Save data to the database

#### Consideration
+ Capable of handling large data files
+ Provide correct transformation
+ Capable of handling scenarios and test ([more info](https://docs.google.com/spreadsheets/d/10420ZdVqh9tX8HHfqCQbzv6Rer4oypdxQ7RoEpgTo7A/edit#gid=506345512))

#### Files:
+ File UserSegment: All code to read/ transform and load file
+ MongoDBConnector: Connector to MongoDB database
+ Schema for read and write dataframe

### 3.3 MongoDB: Datawarehouse - Database
Transformed data are stored in MongoDB
#### Main Tasks:
+ Storing transformed data for further analysis
+ Return queried data to Spark for deduplication/merging process
#### Consideration:
+ Capable of retrieving specific documents -> reduce the amount of data needed to process
+ Support MongoSpark API that utilizes the parallel processing power of spark
#### Tables:
+ Activity: Data related to Transaction's History of users
+ Demographic: Data related to User's Information
+ Promotion: Data related to Promotion campaign and vouchers


<a id='prequesties'></a>
## 4 Prequesties 
Software required to run the project:
+ [Docker](https://www.docker.com)
+ [Scala with Spark](https://spark.apache.org/docs/latest/quick-start.html)
+ [MongoDB](https://www.mongodb.com/try/download/community)
+ [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start/index.html)
+ [Docker-compose]()


<a id='usage'></a>
## 5 Usage


```scala



```


<a id='doc'></a>
## 6 More Documents 
In this sections, additional information related to the project will be provided
+ Clarifying the Input and Output requirements <[link](https://docs.google.com/spreadsheets/d/10420ZdVqh9tX8HHfqCQbzv6Rer4oypdxQ7RoEpgTo7A/edit?usp=sharing)>
+ Graphs in this readme and ERD for Input - Output <[link](https://docs.google.com/document/d/1xtGPNm6DT3w5G9XtnOmor3aAYaLFFzOrUl16pz5kzL8/edit?usp=sharing)>
+ More about the approach <[link](https://docs.google.com/document/d/1xtGPNm6DT3w5G9XtnOmor3aAYaLFFzOrUl16pz5kzL8/edit?usp=sharing)>
+ List of scenarios and Testing methods <[link](https://docs.google.com/spreadsheets/d/10420ZdVqh9tX8HHfqCQbzv6Rer4oypdxQ7RoEpgTo7A/edit?usp=sharing)>

<a id='contributors'></a>
## 7 Contributiors
Member of the team:
1. KhaiT1 -  Team Leader
2. DucNM64 - Team Member
3. Maitp3 - Team Member
