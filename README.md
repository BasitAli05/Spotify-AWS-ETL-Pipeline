# Project Overview
1)	Create New User in IAM and give him S3FullAccess policy and then create access key for third party service
   ![image](https://github.com/user-attachments/assets/d233c6a6-2c6b-4cc5-afbb-0863d2ba0f52)

2)	Using databricks do some transformation and connect with AWS S3
3)	Store that data in S3 staging layer
   ![image](https://github.com/user-attachments/assets/2564454a-791f-49f3-b285-26d78a01311a)
4)	Then using AWS Glue to build ETL pipeline, that will take data from staging layer and transformed it into a datawarehouse layer in S3
   ![image](https://github.com/user-attachments/assets/53aec506-4e9d-464f-9c37-2fad744fcaf1)
5)	Data is stored in partitions in datawarehouse layer
   ![image](https://github.com/user-attachments/assets/e4914735-a863-4220-8282-4e9318dd76c0)
6)	Then create a crawler that will create a database and populate table with data
   ![image](https://github.com/user-attachments/assets/944ade9c-036a-4f9f-95b0-6999e7bf9277)
   ![image](https://github.com/user-attachments/assets/a329404c-7607-4fb8-89f6-559c3b9d817a)
7)	Then using Athena to query the data present in database
   ![image](https://github.com/user-attachments/assets/98bbd10e-4850-4c80-ba33-b1f176b87310)
   ![image](https://github.com/user-attachments/assets/4d4a63b4-400d-4a97-bc90-119cd4ff6659)






