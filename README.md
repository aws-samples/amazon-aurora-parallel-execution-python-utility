## My Project

Version:

Python 3.6 or higher

Description:

The utility is an implementation of applying inserts or updates to tables on Aurora cluster by taking chunks of table rows and assigning them to parallel threads for faster processing.

Customer Use-Case:

From an oracle based Subscriber Management System, The data is being replicated to Aurora Postgres every 30 minutes through AWS DMS and the goal is to process all of it to target tables under the 30-minute SLA. Initially, The ETL was being done through stored procedures in batches which ran fine for smaller jobs but created a bottleneck for jobs with larger data volumes such as a stage table having 40 M rows updating a target table with 1.2 B records.

Aurora Postgres, As of now, Does not offer the capability of parallel writers and the only way to enhance capacity of the cluster is to scale vertically. After trying multiple routes to improve performance such as use of indexes, implement looping in stored procedures to sequentially process small chucks instead of one large insert or update etc. We decided to resolve the bottleneck by developing a python utility that can generate configurable threads and process chunks of rows in parallel. This has been successfully implemented to Production and improved the performance by more than 50 % e.g. One of the updates that took 6 hours now takes 18 minutes to process successfully.


Caution: Although for a shorter time, The resources such as cpu utilization, free-able memory etc. on the cluster can grow very huge and have a potential to hit their max limit if not carefully evaluated. Please make sure to test on a performance cluster before implementing it on production and discuss with DBAs on the configuration of maximum thread count based on your specific system needs.

Key Features:

A) Parallel Execution of data chucks assigned to threads for faster processing. The parameters for rows/thread and max. No of threads at a point and time are configurable.

B) Retry Logic: In case executions have failed for certain threads, The retry mechanism will make sure to make sure all rows are processed successfully.

C) Database/File Logging: the utility will automatically generate a log file based on date and time of the execution that logs all the necessary succeeded executions/exceptions and help troubleshoot in case of failures. In addition, It logs the success/failure status in a database log table which can be eliminated if not a customer requirement.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

