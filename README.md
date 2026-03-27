# **ETL Pipeline using Airflow, MongoDB & Docker**

**🏗️ Architecture Overview**

This project leverages Docker to orchestrate a multi-stage data pipeline that transforms raw TikTok reviews into a structured, cleaned format for NoSQL storage.

🔄 DAG 1: **Data Transformation**

**Sensor:** Monitors the /data folder for the review CSV file.

Branching: A BranchPythonOperator checks if the file is empty.

If Empty: Logs a warning and stops.

If Data: Triggers the Cleaning TaskGroup.

**TaskGroup (Transformation):**

✅ Null Handling: Replaces null with -.

📅 Sorting: Orders all records by created_date.

✨ Sanitization: Removes emojis/symbols via Regex.

**Dataset Outlet:** Automatically signals that the "Processed" file is ready.

**📥 DAG 2: MongoDB Loader**
**Data-Aware Trigger:** Automatically starts only when DAG 1 updates the processed dataset.

**NoSQL Load:** Moves the final cleaned dataset into a MongoDB collection.

**📈 MongoDB Analytics**
Once the pipeline completes, the following insights can be extracted using MongoDB Compass or mongosh:

**Query Goal	MongoDB Logic (Aggregation)**

**Top 5 Comments**	[{$group: {_id: "$content", count: {$sum: 1}}}, {$sort: {count: -1}}, {$limit: 5}]
Short Reviews	{$expr: {$lt: [{$strLenCP: {$toString: "$content"}}, 5]}}

**Daily Avg Rating**	[{$group: {_id: "$created_date", avg: {$avg: "$score"}}}, {$sort: {_id: 1}}]

**🛠️ Quick Setup**

Spin up containers: docker-compose up -d

Configure Airflow: Create a File (path) connection named fs_default pointing to /opt/airflow/data.

Process Data: Drop tiktok_google_play_reviews.csv into your local /data folder and watch the DAGs trigger.
