Data Pipelines with Airflow and AWS
===========================

This project is part of the [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) program, from Udacity. I manipulate data for a music streaming app called Sparkify, where I use Apache Airflow to introduce more automation and monitoring to their data warehouse ETL pipelines.

I create data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. I also implement tests against the datasets after the ETL steps have been executed to catch any discrepancies in the database.

The source data resides in S3 and needs to be processed in a data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


### Install
To set up your python environment to run the code in this repository, start by
 creating a new environment with Anaconda and install the dependencies.

```shell
$ conda create --name ngym36 python=3.6
$ source activate ngym36
$ pip install -r requirements.txt
```

### Run
In a terminal or command window, navigate to the top-level project directory (that contains this README). You need to set up a [Redshift](https://aws.amazon.com/pt/redshift/) cluster. So, start by renaming the file `confs/dpipe.template.cfg` to  `confs/dpipe.cfg` and fill in the `KEY` and `SECRET` in the AWS section. Then, run the following commands:

```shell
$ python iac.py -i
$ python iac.py -r
$ watch -n 15 'python iac.py -s'
```

The above instructions will create the IAM role, the Redshift cluster, and check the status of this cluster every 15 seconds. Fill in the other fields from your `dpipe.cfg` that shows up in the commands console outputs. After Amazon finally launch your cluster, run:

```shell
$ python iac.py -t
$ . setup-airflow.sh
$ python iac.py -a
$ . start-ariflow.sh
```

The first command opens a TCP port to your cluster so that you can manipulate data from outside. The second command sets your AIRFLOW_HOME to the `airflow/` folder in the current project. **Some errors will show up, don’t worry**. The third command sets the required variables and connections, as the Redshift host address and AWS key. The last command starts the airflow UI.

Then, navigate to `http://localhost:3000` in your browser and turn on the `etl_dag`. It will create all the tables and insert data from S3 to the staging, dimension, and fact tables. You can click in the DAG name to follow the process execution steps. Finally, CLEAN UP your resources using the commands below:

```shell
$ python iac.py -d
$ watch -n 15 'python iac.py -s'
```

Wait for the second command to fail to find the cluster. **Redshift is expensive.**

### License
The contents of this repository are covered under the [MIT License](LICENSE).
