# Data Extraction - Final Project
This repo was created to host my final project for Ada Tech's, Santander Coders, Data Extraction course.

## How to use this repo
Bellow are some instructions for running the scripts in this repo.

### Common steps
* Clone this repo to your machine;
* In */dags/scripts/news.py*, add your GNews API key to *api_key*.
    * You can get one in **https://gnews.io/**.
* In */dags/scripts/sentiment.py*, add your Meaning Cloud API key to *api_key*.
    * You can get one in **https://www.meaningcloud.com/**.

#### Running locally
* Run *news.py* first so that a *raw.parquet* file is created in */dags/data/*;
* Run *sentiment.py* second so that it loads *raw.parquet* and creates a *sentiment.parquet* file, also in */dags/data/*;
* Optionally, make use of */dags/debug/display_df.ipynb* to load one of the parquet files and display its content.

#### Running on Airflow
* Add the **absolute path** to your Airflow dags folder to *path_to_dags_folder* variable in the three following *.py* files:
    * */dags/sentiment_analysis.py*,
    * */dags/scripts/news.py*,
    * */dags/scripts/sentiment.py*.
* Copy all the contents of */dags* to your airflow dags folder;
* Start Airflow.