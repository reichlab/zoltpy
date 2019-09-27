# Zoltpy
A python module that interfaces with Zoltar https://github.com/reichlab/forecast-repository

## Installation requirements
- [python 3.6](https://www.python.org/downloads/release/python-360/) 
- [pipenv](https://pipenv.readthedocs.io/en/latest/) for managing packages - see Pipfile
- [click](https://click.palletsprojects.com/en/7.x/) - for the demo application's handling of args
- [pandas](https://pandas.pydata.org/) - for use of dataframe function
- [requests](http://docs.python-requests.org/en/v2.7.0/user/install/)
- [numpy](https://pypi.org/project/numpy/)

## Installation
Zoltpy is hosted on the Python Package Index (pypi.org), a repository for Python modules https://pypi.org/project/zoltpy/. 

Install Zoltpy with the following command:
```
pip install zoltpy
```

## One-time configuration
Users must add their Zoltar username and password to environment variables on their machine before using this module. 

### For Mac/Unix
```
cd ~
nano .bash_profile
```
Add the following to your bash_profile:
```
export Z_USERNAME=<your zoltar username>
export Z_PASSWORD=<your zoltar password>
```
After you are finished, press `Ctrl` + `O`, `Enter`, and `Ctrl` + `X` to save and quit.

Then enter the command:
```
source ~/.bash_profile
```
To ensure your environment variable is configured properly, run this command and check for Z_USERNAME and Z_PASSWORD:
```
printenv
```

### For PC
In the command prompt, run the following commands:
```
set Z_USERNAME="<your zoltar username>"
set Z_PASSWORD="<your zoltar password>"
```

## Usage
Zoltpy is a python module that communicates with Zoltar, the Reich Lab's forecast repository. To import the Zoltpy utility functions functions, run the following command after installing the package:
```
from zoltpy import util
```

### Zoltpy currently has 5 Key Functions:
1) [print_projects()](#print-project-names) - Print project names
2) [print_models(`project_name`)](#print-model-names) - Print model names for a specified project
3) [delete_forecast(`project_name`, `model_name`, `timezero_date`)](#delete-forecast) - Deletes a forecast from Zoltar
4) [upload_forecast(`project_name`, `model_name`, `timezero_date`, `forecast_csv_file`)](#Upload-a-Forecast) - Upload a forecast to Zoltar
5) [forecast_to_dataframe(`project_name`, `model_name`, `timezero_date`)](#Return-Forecast-as-a-Pandas-Dataframe) - Returns forecast as a Pandas Dataframe


#### Print Project Names
This fuction returns the project names that you have authorization to view in Zoltar.
```
util.print_projects()
```

#### Print Model Names
Given a project, this function prints the models in that project.
```
util.print_models(project_name = 'My Project')
```

#### Delete a Forecast
Deletes a single forecast for a specified model and timezero.
```
util.delete_forecast(project_name='My Project', model_name='My Model', timezero_date='YYYYMMDD')
```
Example:
```
util.delete_forecast('Impetus Province Forecasts','gam_lag1_tops3','20181203')
```

#### Upload a Forecast
```
from zoltpy import util

project_name = 'private project'
model_name = 'Test ForecastModel1'
timezero_date = '20170117'
forecast_file_path = 'tests/EW1-KoTsarima-2017-01-17-small.csv'

forecast_filename, predx_json = util.convert_cdc_csv_to_predx_json(forecast_file_path)
conn = util.authenticate()
util.upload_forecast(conn, predx_json, forecast_filename, project_name, model_name, timezero_date, overwrite=True)
```

#### Return Forecast as a Pandas Dataframe
Example:
```
util.forecast_to_dataframe('Impetus Province Forecasts','gam_lag1_tops3','20181203')
```
