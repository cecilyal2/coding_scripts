```python
import pandas as pd
import time
import pyodbc
from sqlalchemy import create_engine
from pandas import ExcelWriter
from pandas import ExcelFile
import datetime as dt
from email.message import EmailMessage
import smtplib, ssl
```


```python
# parameters for which server and database you want to pull data from
DB = {'servername': '',
      'database': ''}

# create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
```


```python
sql = '''select * 
from sensitive.dbo.emailnotification_dailystatus
where job_rundate = cast(getdate() as date)'''
```


```python
df = pd.DataFrame(pd.read_sql(sql, conn))
```


```python
df.job_name[1]
```




    'SSIS-ETL-Email Notification for CC Supervisor - daily 7:00 am'




```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>job_status</th>
      <th>job_name</th>
      <th>job_rundate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>Job Failed</td>
      <td>SSIS-ETL-Email Notification - daily - every 10...</td>
      <td>2021-03-26</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Job Ran Successfully</td>
      <td>SSIS-ETL-Email Notification for CC Supervisor ...</td>
      <td>2021-03-26</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Job Failed</td>
      <td>SSIS-ETL-Email Notification - daily - every 10...</td>
      <td>2021-03-26</td>
    </tr>
    <tr>
      <td>3</td>
      <td>Job Ran Successfully</td>
      <td>SSIS-ETL-Email Notification for CC Supervisor ...</td>
      <td>2021-03-26</td>
    </tr>
    <tr>
      <td>4</td>
      <td>Job Failed</td>
      <td>SSIS-ETL-Email Notification - daily - every 10...</td>
      <td>2021-03-26</td>
    </tr>
    <tr>
      <td>5</td>
      <td>Job Ran Successfully</td>
      <td>SSIS-ETL-Email Notification for CC Supervisor ...</td>
      <td>2021-03-26</td>
    </tr>
  </tbody>
</table>
</div>




```python
for x in range(len(df.job_status)):
    if df.job_status[x] == 'Job Failed' and df.job_name[x] == 'SSIS-ETL-Email Notification - daily - every 10 seconds':
        email_body = 'SSIS-ETL-Email Notification - daily - every 10 seconds has failed.'
        msg = EmailMessage()
        msg.set_content(email_body)
        msg["Subject"] = "Email Notification ETL Failure"
        msg["From"] = "email@email.org"
        msg["To"] = ["email@email.org"]

        context=ssl.create_default_context()

        with smtplib.SMTP("smtp.office365.com", port=587) as smtp:
            smtp.starttls(context=context)
            smtp.login(msg["From"], 'password')
            smtp.send_message(msg)
            
    elif df.job_status[x] == 'Job Failed' and df.job_name[x] == 'SSIS-ETL-Email Notification for CC Supervisor - daily 7:00 am':
        email_body = 'SSIS-ETL-Email Notification for CC Supervisor - daily 7:00 am has failed.'
        msg = EmailMessage()
        msg.set_content(email_body)
        msg["Subject"] = "Email Notification CC Supervisor ETL Failure"
        msg["From"] = "email@email.org"
        msg["To"] = ["email@email.org"]

        context=ssl.create_default_context()

        with smtplib.SMTP("smtp.office365.com", port=587) as smtp:
            smtp.starttls(context=context)
            smtp.login(msg["From"], 'password')
            smtp.send_message(msg)
            
        
                        
```


```python

```


```python

```


```python

```


```python

```


```python

```
