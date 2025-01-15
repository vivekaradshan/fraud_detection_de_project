##Build an application for fraud detection

###Step 1: Ingest data using Kafka
Data source to Kafka producer
1. Point of Sale (POS) Transactions
2. Online Transactions
3. ATM Transactions
4. Recurring Payments
5. Bank Transfers

###Step 2: Consume data using flink and spark streaming
1. Flink to run realtime machine learning pipeline
2. Spark streaming to parse the data and store in S3

###Step 3: Build dashboard for realtime insights
1.  Store the data in elasticsearch 
2.  Build realtime dashboard in Kibana