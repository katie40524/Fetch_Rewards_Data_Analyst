## Fetch_Rewards_Data_Analyst

### First: Review Existing Unstructured Data and Diagram a New Structured Relational Data Model

For the three json files, I explode them using Python and export them to csv files (Fetch_Rewards.ipynb). For rewardsReceiptItemList column in receipts table, there are more than one elements in this column so I extract those elements from the original column into new colimns. After exploding them, I find that the rewardsReceiptItemList_brandCode column can merge with the brandCode column in Brand table to have more unformation about the relationship for receipts and brand. Therefore, I connect these two column with the key brandCode and rewardsReceiptItemList_brandCode. For Users table, I connect Users table with Receipts table on Users._id and Receipts.userId. 

![ER_diagram](https://user-images.githubusercontent.com/62042891/172951436-b06f0444-b6c5-4fa2-8e79-47a2aa19accc.png)

### Second: Write a query that directly answers a predetermined question from a business stakeholder

In order to execute SQL in Python, I use PySpark on Databricks to import the csv files and write the SQL query (.py).

> What are the top 5 brands by receipts scanned for most recent month?

<img width="637" alt="截圖 2022-06-09 下午5 43 29" src="https://user-images.githubusercontent.com/62042891/172950366-0a838fad-890b-4fee-9231-732ed827ee47.png">

According to the result, we can find out that the top five brands for receipts scanned for most recent month are HY-VEE, PEPSI, KROGER, KLEENEX and BEN AND JERRYS.

### Third: Evaluate Data Quality Issues in the Data Provided

The first issue I find that in Users table is that there are mutiple duplicate createdDate and lastLogin in the same user id. Some id has 20 duplicate  records. It will be an issue for the system storage.

![截圖 2022-06-10 上午11 38 42](https://user-images.githubusercontent.com/62042891/173101357-774d7795-cf2c-48b8-b600-c2625d6bee35.png)

Another problem is that I can't find the brandCode in Brand table that has a record in rewardsReceiptItemList_brandCode of receipts table. It migth be an issue if we want to find other information from the brandCode from the brand table and there is no information in brand table.

![截圖 2022-06-10 上午11 42 27](https://user-images.githubusercontent.com/62042891/173102083-a98f9e8c-7612-4889-bad7-68b150f65b5a.png)



### Fourth: Communicate with Stakeholders

Hi everyone, 

Hope you all are doing well! I am wiring this message because I have finished working on Fetch Rewards data which are receipts, brand, and users data, and want to talk about the issues I found in the data. Before talking about the issues, I would like to make a brief introduction of how I deal with the data. The three data files are JSON files, so I used Python to clean those data and exploded columns if there are multiple elements in one column. Then, I export the three data into a CSV file which becomes a structured format. Finally, I executed these data in Databricks using PySpark to check if there are problems with these data. After checking the data, I found that

-	In the receipts and brand table, there are columns containing multiple elements which will be obstacles while managing those data. If this issue is solved, it might increase the efficiency and performance of data cleaning and data analysis. 

-	Besides, when I tried to merge receipts and brand tables using brandCode, I realized that there are many brandCode exist in the receipts table but not in the brand table. I was wondering if it might have some issue when collecting information about brandCode. 

-	There are many duplicate data in the users table. I am curious why will this situation happens and am concerned that it will be an issue for the storage system since there are many duplicate data storages.

-	There is many missing information in the users table such as SignupSource and State. This missing information won’t hinder data preformation, but it might decrease the performance while we want to analyze details from our data, for example, if we want to know our consumer distribution and make predictions based on the distribution, the analysis will be more complete if we have enough data.

I would like to discuss with you to solve those problems and schedule a meeting to discuss those issues. If there is any question about these problems, feel free to contact me and we can discuss them in detail. Thanks!

Sincerely,
Katie

