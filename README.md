# Zoltpy
A python module that interfaces with Zoltar https://github.com/reichlab/forecast-repository

## Installation requirements
- [pipenv](https://pipenv.readthedocs.io/en/latest/) for managing packages - see Pipfile
- [click](https://click.palletsprojects.com/en/7.x/) - for the demo application's handling of args
- [pandas](https://pandas.pydata.org/) - for use of dataframe function

## Installation
Zoltpy is hosted on the Python Package Index (pypi.org), a repository for Python modules https://pypi.org/project/zoltpy/. 

Install Zoltpy with the following command:
```
pip install zoltpy
```

## One-time configuration
Users must add their Zoltar username and password to environment variables on their machine before using this module. 

*For Mac Users*
```
cd ~
nano .bash_profile
```
Add the following to your bash_profile:
```
export DEV_PASSWORD=<your zoltar password>
export DEV_USERNAME=<your zoltar username>
```
To save the file, 
hold `shift:` 
then type: `wq+Enter`

*For PC Users*
In the command prompt, run the following commands:
```
set DEV_PASSWORD="<your zoltar password>"
set DEV_USERNAME="<your zoltar username>"
```

## Usage
Zoltpy is a python module that communicates with Zoltar, the Reich Lab's forecast repository. To import the Zoltpy functions, run the following command after installing the package:
```
from zoltpy import functions
```

**Zoltpy currently has 5 Key Functions:**
1) [print_projects()](#print-project-names) - Print project names
2) [print_models(`project_name`)](#print-model-names) - Print model names for a specified project
3) [delete_forecast(`project_name`, `model_name`, `timezero_date`)](#delete-forecast) - Deletes a forecast from Zoltar
4) [upload_forecast(`project_name`, `model_name`, `timezero_date`, `forecast_csv_file`)](#Upload-a-Forecast) - Upload a forecast to Zoltar
5) [forecast_to_dataframe(`project_name`, `model_name`, `timezero_date`)](#Return-Forecast-as-a-Pandas-Dataframe) - Returns forecast as a Pandas Dataframe


### Print Project Names
This fuction returns the project names that you have authorization to view in Zoltar.
```
functions.print_projects()
```

### Print Model Names
Given a project, this function prints the models in that project.
```
functions.print_models(project_name = 'My Project')
```

### Delete a Forecast
Deletes a single forecast for a specified model and timezero.
```
functions.delete_forecast(project_name='My Project', model_name='My Model', timezero_date='YYYYMMDD')
```
Example:
```
functions.delete_forecast('Impetus Province Forecasts','gam_lag1_tops3','20181203')
```

### Upload a Forecast
```
functions.upload_forecast(project_name='My Project', model_name='My Model', timezero_date='YYYYMMDD', 'C:\\Users\\house\\Desktop\\20181203-gam_lag1_tops3-20190114.csv')
```

Example:
```
functions.upload_forecast('Impetus Province Forecasts','gam_lag1_tops3','20181203','C:\\Users\\house\\Desktop\\20181203-gam_lag1_tops3-20190114.csv')
```

### Return Forecast as a Pandas Dataframe
Example:
```
functions.forecast_to_dataframe('Impetus Province Forecasts','gam_lag1_tops3','20181203')
```
