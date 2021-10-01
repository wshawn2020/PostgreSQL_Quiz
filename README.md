# Epoch Data Engineer Assignment

This assignment is an opportunity for you to demonstrate your relevant skills for the
SLP Data Engineer role.

You will produce a set of Bash and Python scripts that will:
* Start PostgreSQL database in a Docker container
* Create an appropriate table for persisting the data described below
* Insert data from compressed CSV into the created table
* Query the database to produce a new CSV with summary data

## Requirements
In order to complete this task, you will need to use:
* Bash
* Docker
* Gzip tools (optional, you could use Python stdlib)
* Python
    * Conda
    * PsycoPg2
    * Pandas

## Input Data
There is a compressed CSV file within this repository [data.csv.gz](data.csv.gz) which contains open, high, low
and close prices as well as total traded shares for a set of stocks that trade on the ASX exchange.

The columns are:
* date
* symbol
* name
* open_price
* high_price
* low_price
* close_price
* volume

## Output Data
The output data is expected to be in CSV format, with the following schema:
    `<symbol>,<mean_change_pct>,<max_high_price>,<min_low_price>,<median_volume>`

Where the computed values are:
* `<mean_change_pct>` The percentage change between `<close_price>` for a given symbol and the preceeding date's value
* `<max_high_price>` The maximum `<high_price>` for a given symbol over all data
* `<min_low_price>` The minimum `<low_price>` for a given symbol over all data
* `<median_volume>` The median `<volume>` for a given symbol over all data

## Inclusions

The following describe some included files that will help you get started, you may edit these as you wish or use another
method. Be sure to include anything else you use.

## docker-compose.yml
We have included a [docker-compose.yml](docker-compose.yml) file which will bootstrap a PostgreSQL instance including an Adminer Web UI
that may be useful for you to debug your data.

PostgreSQL will be listening to default port `5432` and Adminer to port `8080`.

## env.yml
We have included a [env.yml](env.yml) Conda environment file that you may use to bootstrap your Python dependencies.
If you do not already have Conda, you can obtain it at [conda.io](https://conda.io).

## Other
Please submit your source code to this repository, and amend this README file to include any instructions or other notes
that you wish to provide.

If you have any questions, please feel free to ask and we will assist where we can.

# Feedback from Shawn
## Keypoints to mention
It's finished building up the pipeline for launching environment, handling table in PostgreSQL database and exporting 
csv file. 

There's a bash script called `<launch_pipeline.sh>` which can automatically finish all the processes just enter simple
command `./launch_pipeline.sh` in bash shell under project's root path.

If it's going to make adaptions manually, all the python scripts located in `folder script`. The entry main function is
in `./script/main.py`. The definition of `DATABASE` is in `./script/database.py`.

Once finish all the processes of the pipeline, the export csv file can be found under folder path `./export/report.csv`.

For persisting data in database, there are tiny adaptions in `docker-compose.yml`. It's just creating a volume for 
that and mounting to host path. 

## Environment
It's suggest that making sure environment dependencies listed below intalled successfully in advance for running bash 
script `<launch_pipeline.sh>`.

Dependencies:
* bash
* conda
* docker