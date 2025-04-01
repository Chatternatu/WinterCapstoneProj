"# WinterCapstoneProj" 
This project is carried out by ingesting files from AWS s3 bucket to the snowflake environment. 
By creating warehouse, database, as well as table schema which matches that of the data source, then setting up an integration pipeline which enabled me connect to the s3 storage, with the file format being defined, I went ahead to create the staging  table in
snowflakes to  copy data from source into the staging table created in my snowflake environment.
Finally, I set up an automated task that schedule on refresh.
