# Zoltpy
A python module that interfaces with Zoltar https://github.com/reichlab/forecast-repository

## Installation requirements
- [python 3.6](https://www.python.org/downloads/release/python-360/) 
- [pipenv](https://pipenv.readthedocs.io/en/latest/) for managing packages - see Pipfile
- [click](https://click.palletsprojects.com/en/7.x/) - for output, and for the demo application's handling of args
- [pandas](https://pandas.pydata.org/) - for use of dataframe function
- [requests](http://docs.python-requests.org/en/v2.7.0/user/install/)
- [numpy](https://pypi.org/project/numpy/)

## Installation
Zoltpy is hosted on the Python Package Index (pypi.org), a repository for Python modules https://pypi.org/project/zoltpy/. 

Install Zoltpy with the following command:
```
pip install git+https://github.com/reichlab/zoltpy/
```

## One-time Environment Variable Configuration
Users must add their Zoltar username and password to environment variables on their machine before using this module. 

#### For Mac/Unix
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

#### For PC
In the command prompt, run the following commands:
```
set Z_USERNAME="<your zoltar username>"
set Z_PASSWORD="<your zoltar password>"
```

## Usage
Zoltpy is a python module that communicates with Zoltar, the Reich Lab's forecast repository. To import the Zoltpy utility functions, run the following command after installing the package:
```
from zoltpy import util
```

## Authentication
To access your project, you'll first need to authenticate via the `authenticate(username, password)` method from the `ZoltarConnection()` object. Pass it the username and password saved in your [environment variables](#one-time-environment-variable-configuration): 
```
from zoltpy import util

conn = util.authenticate()
```
Now you can use your authentication token to access private projects:
```
project = [project for project in conn.projects]
print(project)
```
- Be careful to store and use your username and password so that they're not accessible to others. The preferred method is to [create enviornment variables](#one-time-environment-variable-configuration)
- The Zoltar service uses a "token"-based scheme for authentication. These tokens have a five minute expiration for
  security, which requires re-authentication after that period of time. The Zoltpy library takes care of 
  re-authenticating as needed by passing your username and password back to the server to get another token. Note that
  the connection object returned by the `re_authenticate_if_necessary()` function stores a token internally, so be careful if saving that object into a file.
  
  
## Zoltpy currently has 4 Key Functions
1) [print_projects()](#print-project-names) - Print project names
2) [print_models(`conn`,`project_name`)](#print-model-names) - Print model names for a specified project
3) [delete_forecast(`conn`, `project_name`, `model_abbr`, `timezero_date`)](#delete-forecast) - Deletes a forecast from Zoltar
4) [upload_forecast(`conn`, `project_name`, `model_abbr`, `timezero_date`, `forecast_csv_file`)](#Upload-a-Forecast) - Upload a forecast to Zoltar


### Print Project Names
This function returns the project names that you have authorization to view in Zoltar.
```
util.print_projects()
```

### Print Model Names
Given a project, this function prints the models in that project.
```
util.print_models(conn, project_name = 'My Project')
```

### Delete a Forecast
Deletes a single forecast for a specified model and timezero.
```
util.delete_forecast(conn, project_name='My Project', model_abbr='My Model', timezero_date='YYYY-MM-DD')
```
Example:
```
conn = util.authenticate()

util.delete_forecast(conn, `'Impetus Province Forecasts','gam_lag1_tops3','20181203')
```

### Upload a Single Forecast
```
project_name = 'Docs Example Project'
model_abbr = 'docs forecast model'
timezero_date = '2011-10-09'
predx_json_file = 'examples/docs-predictions.json'
forecast_filename = 'docs-predictions'

conn = util.authenticate()

util.upload_forecast(conn, predx_json_file, forecast_filename, project_name, model_abbr, timezero_date overwrite=True)
```

### Uploading Multiple Forecasts
This method makes uploading multiple forecasts for a single model and project more efficient. The first step is to iterate through every forecast in your model and create the following three batch variables: `predx_batch`, `forecast_filename_batch`, `timezero_batch`. Below is an example of getting these batch variables
```
# import libraries
import pymmwr as pm
from zoltpy import util
import datetime

# initialize parameters
project_name = 'private project'
model_abbr = 'Test ForecastModel1'

# set up batch variables
predx_batch = []
forecast_filename_batch = []
timezero_batch = []

for csv_file in '/Users/my/forecast/directory':
    conn = util.authenticate()
    
    # get timezero
    timezero = pm.epiweek_to_date(ew)
    timezero = timezero + datetime.timedelta(days = 1) # timezeros on Mondays
    timezero = timezero.strftime('%Y%m%d')

    # generate predx_json and forecast_filename
    predx_json, forecast_filename = util.convert_cdc_csv_to_json_io_dict(2016, csv_file)
    
    # save batch variables
    predx_batch += [predx_json]
    forecast_filename_batch += [forecast_filename]
    timezero_batch += [timezero]

util.upload_forecast_batch(conn, predx_batch, forecast_filename_batch, project_name, model_abbr, timezero_batch,
                           overwrite=False)
```

### Download a forecast

```python
conn = util.authenticate()
project_name = 'Docs Example Project'
model_abbr = 'docs forecast model'
timezero_date = '2011-10-09'
json_io_dict = download_forecast(conn, project_name, model_abbr, timezero_date)
print(f"downloaded {len(json_io_dict['predictions'])} predictions")
```


### Return Forecast as a Pandas Dataframe

```python
df = dataframe_from_json_io_dict(json_io_dict)
print(f"dataframe:\n{df}")
```
