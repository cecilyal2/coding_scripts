```python
import pandas as pd
import sqlalchemy as sql
import pyodbc
import time
import smtplib
from email.mime.text import MIMEText
import datetime as dt
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import sys
```


```python
# parameters for which server and database you want to pull data from
DB = {'servername': '',
      'database': ''}

# create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')

```


```python
query = '''
SELECT sj.job_id,
sjs.name,
step_name,
last_run_date,
 sj.last_run_outcome
  FROM [msdb].dbo.sysjobsteps sj
    inner join [msdb].[dbo].[sysjobs] sjs
 on sj.job_id = sjs.job_id

  where enabled = 1
--this and clause shows if the job failed or not, 0 failed, 1 residental
  and sj.last_run_outcome = 0
'''
```


```python
df_main = pd.read_sql(query, conn)
```


```python
df_main
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
      <th>job_id</th>
      <th>name</th>
      <th>step_name</th>
      <th>last_run_date</th>
      <th>last_run_outcome</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>7284A27D-2B2E-4290-8861-05B8372AC126</td>
      <td>CopyReportingtoDev_usingDetach</td>
      <td>CopyReportingtoDev_usingDetach_Step</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td>1</td>
      <td>C7948232-797F-48DF-A421-829466E1E3CC</td>
      <td>Check Weekly Extract deltas BI01 to Avatar Sun...</td>
      <td>Check weekly</td>
      <td>20220116</td>
      <td>0</td>
    </tr>
    <tr>
      <td>2</td>
      <td>B6052CDA-940D-49B8-9810-B49A42F6BFEE</td>
      <td>SSIS-ETL-AvSearch 7:31 daily</td>
      <td>Run SSIS ETL-AvSearchSPs package</td>
      <td>20220120</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_main = df_main[['name','step_name', 'last_run_date']]
```


```python
df_main
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
      <th>name</th>
      <th>step_name</th>
      <th>last_run_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>CopyReportingtoDev_usingDetach</td>
      <td>CopyReportingtoDev_usingDetach_Step</td>
      <td>0</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Check Weekly Extract deltas BI01 to Avatar Sun...</td>
      <td>Check weekly</td>
      <td>20220116</td>
    </tr>
    <tr>
      <td>2</td>
      <td>SSIS-ETL-AvSearch 7:31 daily</td>
      <td>Run SSIS ETL-AvSearchSPs package</td>
      <td>20220120</td>
    </tr>
  </tbody>
</table>
</div>




```python
todays_date = time.strftime("%Y-%m-%d")
```


```python
todays_date
```




    '2022-01-21'




```python
fromaddr = "email@gmail.org"
toaddr = 'email@email.org'
password = 'password'
subject = 'ETL Failure for ' + todays_date
```


```python
subject
```




    'ETL Failure for 2022-01-20'




```python
data_msg = df_main.to_html()
```


```python
HTMLBody = '''<h3>Please find data attached and below.</h3>
                   {}'''.format(df_main.to_html())
```


```python
html = """\
<html>
  <head></head>
  <body>
    {0}
  </body>
</html>
""".format(df_main.to_html())

#df_attach = MIMEText(html, 'html')
#msg.attach(df_attach)
```


```python
df_main
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
      <th>name</th>
      <th>step_name</th>
      <th>last_run_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>CopyReportingtoDev_usingDetach</td>
      <td>CopyReportingtoDev_usingDetach_Step</td>
      <td>0</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Check Weekly Extract deltas BI01 to Avatar Sun...</td>
      <td>Check weekly</td>
      <td>20220116</td>
    </tr>
    <tr>
      <td>2</td>
      <td>SSIS-ETL-AvSearch 7:31 daily</td>
      <td>Run SSIS ETL-AvSearchSPs package</td>
      <td>20220120</td>
    </tr>
  </tbody>
</table>
</div>




```python
#'', text, 
```


```python
text = 'Hello,\n\n    The ETLs below have failed today. ' +  'Please look at file to see if column names changed. \n\n Thank you, \n      Information Systems'
body = '\r\n'.join(['To: %s' % toaddr,
                    'From: %s' % fromaddr,
                    'Subject: %s' % subject,
                    '', df_main])
server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.connect("smtp-mail.outlook.com",587)
server.ehlo()
server.starttls()
server.ehlo()
server.login(fromaddr, password)
#text = msg.as_string()
server.sendmail(fromaddr, toaddr, body)
```


    ---------------------------------------------------------------------------

    TypeError                                 Traceback (most recent call last)

    <ipython-input-46-cb07156cb6a1> in <module>
          3                     'From: %s' % fromaddr,
          4                     'Subject: %s' % subject,
    ----> 5                     '', df_main])
          6 server = smtplib.SMTP('smtp-mail.outlook.com', 587)
          7 server.connect("smtp-mail.outlook.com",587)


    TypeError: sequence item 4: expected str instance, DataFrame found



```python
server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.starttls()
server.login(fromaddr , password)

msg = df_main.to_html()
server.sendmail(fromaddr, toaddr, msg)
server.quit()
```




    (221, b'2.0.0 Service closing transmission channel')




```python
recipients = ['email@email.org'] 
emaillist = [elem.strip().split(',') for elem in recipients]
msg = MIMEMultipart()
msg['Subject'] = "ETL Failures for " + todays_date
msg['From'] = 'email@email.org'

body = 'Hello,\n\n    The ETLs below have failed today. ' +  '\n\n Thank you, \n      Information Systems'

#body = MIMEText(body) # convert the body to a MIME compatible string
#msg.attach(body)

html = """\
<html>
  <head></head>
  <body>
    {0}
  </body>
</html>
""".format(df_main.to_html())

part1 = MIMEText(html, 'html')
msg.attach(part1)

server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.connect("smtp-mail.outlook.com",587)
server.ehlo()
server.starttls()
server.ehlo()
server.login(fromaddr, password)
```




    (235, b'2.7.0 Authentication successful')




```python
server.sendmail(msg['From'], emaillist, msg.as_string())
```




    {}




```python

```
