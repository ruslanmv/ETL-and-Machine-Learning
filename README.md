# ETL and Machine Learning 

*How to create an ETL pipeline  with Machine Learning  by using Airflow and Spark*. 

We are going to build an ETL pipeline  by using **Airflow** with **Jupyter Lab**.  We will describe all the steps  to create the ETL.  

First we  will create a Python environment with all needed tools. Then we are going to create different notebooks of  the ETL, such as the extraction of the data, transformation of the data , creation of the Machine Learning Model and the deployment. 

## Contents

1. Setup of the Environment
2. Pulling the data in `CSV` format
3. Covert the `CSV` file to `parquet` format.
4. Reading  the `parquet` data
5. Covert the `parquet` file to `CSV` format.
6.  Loading the CSV file into a RDD Dataframe and Hyper Parameter tunning.
8. Machine Learning Model Creation
9. Creation of the Pipeline
10. Machine Learning Deployment

##  Requirements

- Python 3.8
- NodeJS 16.11 
- Anaconda
- Git
- Desire to learn :)

## Step 1. Setup of the Environment



I present two ways to create your Environment 

a) Docker Installation ( easy )

b) Standard Installation 

You can choose **just one option** to follow:



###  a) Docker Installation 

First you have to download and install docker desktop [here](https://www.docker.com/products/docker-desktop)

[![](assets/images/posts/README/down-16342373633162.jpg)](https://www.docker.com/products/docker-desktop)

after the installation is done, and docker is running,   go to your terminal and type the following command:

```
docker run  -p 8888:8888 ruslanmv/pyspark-elyra:3.0.2
```

you will get something like

![](assets/images/posts/README/run.jpg)



then copy your last url ,something similar like  `http://127.0.0.1:8888/lab?token=5fb30b71e29ad77cb92bcfaa7a546ddb6b26f5e92ebeabbb` and paste in your browser

![](assets/images/posts/README/jupyterlab.jpg)

then open the Terminal of JupyterLab 

![](assets/images/posts/README/terminal.jpg)

and type the following commands

```
conda create -n etl   python==3.8 findspark  -y
conda activate etl
```

then in your terminal type the following commands:

```
conda install ipykernel -y
python -m ipykernel install --user --name etl --display-name "Python 3.8 - (ETL)"
```

Installing additional libraries needed for this project 

```
pip install pyspark==3.1.2 wget==3.2 pyspark2pmml==0.5.1 ibm-watson-machine-learning==1.0.45
```



```
pip install pyspark ibm-watson-machine-learning==1.0.45 pyspark2pmml wget
```

and finally clone the repository of this project

```
git clone https://github.com/ruslanmv/ETL-and-Machine-Learning.git
```

![](assets/images/posts/README/pyspark2.jpg)

now you have a new folder called `ETL-and-Machine-Learning` you can open it and will see all the folders needed for this project

![](assets/images/posts/README/folders.jpg)

In addition  now you can start the  **Step 2.  Pulling the data**. 



### b) Standard Installation

First we need to install  **Nodejs** 16.11 

In Windows you can install it by using [Chocolatey](https://docs.chocolatey.org/en-us/choco/setup) 

```
choco install nodejs
```

or simply install it  from  [here](https://nodejs.org/en/)

If you are using Ubuntu

```
sudo apt install nodejs
```

We need have installed git, in Windows you can install by using

```
choco install git
```

o Ubuntu

```
sudo apt install git
```

Then we need install **Anaconda** and **Spark** you can follow the instructions from  [here](https://ruslanmv.com/blog/Machine-Learning-with-Python-and-Spark).

We create an environment called **etl** with  **Jupyter lab**  with **Elyra**

```
conda create -n etl python==3.8 findspark jupyterlab  jupyterlab-git elyra
```

```
conda activate etl
```

then we need

```
pip3 install jupyterlab-git==0.30.0 numpy pandas
```

```
pip install ibm-watson-machine-learning==1.0.45
```



```
conda install ipykernel
```

```
python -m ipykernel install --user --name etl --display-name "Python 3.8 - Spark(ETL)"
```

and then

```
jupyter server extension enable elyra
```

Verify that the `elyra` extension is installed.

```
jupyter serverextension list
```

Should output:

```
  True, help=_("try to find known language servers in sys.prefix (and elsewhere)")
      jupyter_lsp 1.4.1 ok
    jupyter_resource_usage enabled
    - Validating...
      jupyter_resource_usage  ok
    jupyterlab enabled
    - Validating...
      jupyterlab 3.1.18 ok
    jupyterlab_git enabled
    - Validating...
      jupyterlab_git 0.30.0 ok
    nbdime enabled
    - Validating...
      nbdime 3.0.0 ok
```

then we open the program by typing

```
jupyter lab
```



![](assets/images/posts/README/wuala.jpg)

Great you have finished the First Step, you have installed the **Pipeline Editors** that we will use. 



![](assets/images/posts/README/editor.jpg)

For additional information of the extensions visit the original source of Elyra [here]( https://elyra.readthedocs.io/en/latest/getting_started/installation.html#conda).



### Installation of Jupyter Lab with Pyspark 

We are going to setup the Jupyter Lab with the Pyspark.



![](assets/images/posts/README/8.jpg)



If your are using ubuntu  You can run your **Jupyter Lab**  with the pyspark command by setting the relevant environment variables:

```
export PYSPARK_DRIVER_PYTHON=jupyter
export IPYTHON=1
export PYSPARK_DRIVER_PYTHON_OPTS="lab"
```

or you can run your **Jupyter notebook** with the pyspark 

```py
export PYSPARK_DRIVER_PYTHON=jupyter
export IPYTHON=1
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --port=XXX --ip=YYY"
```

with XXX being the port you want to use to access the notebook and YYY being the ip address.

In this project we will use only the **Juyter lab**,



### JPMML-SparkML

In this project we will use **spark 3.0.2,** if you will use **3.1+** you should update the commands with the correct JPMML jar names.

We will use a **Java library** and **command-line application** for converting Apache Spark ML pipelines to PMML

| Apache Spark version | JPMML-SparkML branch                                         | Status |
| -------------------- | ------------------------------------------------------------ | ------ |
| 3.0.X                | [`1.6.X`](https://github.com/jpmml/jpmml-sparkml/tree/1.6.X) | Active |

For more information [here](https://github.com/jpmml/jpmml-sparkml) 

Just for complementary information , when you have many jar files  we can link it dynamically via the SparkConf object when you create the SparkSession,

```py
spark = SparkSession \
    .builder \
    .appName("My App") \
    .config("spark.jars", "/path/to/jar.jar,/path/to/another/jar.jar") \
    .getOrCreate()
```

In this project we will use only one jar.



### Opening Jupyter Lab with Pyspark

First we clone the repository that we will work,

```
git clone https://github.com/ruslanmv/ETL-and-Machine-Learning.git
```

Now if everything is correct.  Open your terminal cmd and 

```
cd ETL-and-Machine-Learning
```

```
conda activate etl
```

```
pyspark
```

![](assets/images/posts/README/wuala2.jpg)



### To Set Linux Distro Version to WSL 1 or WSL 2 in Windows 10,

Open [PowerShell as Administrator](https://winaero.com/all-ways-to-open-powershell-in-windows-10/#elevated).

First we check our Linux Distro in Windows 

```
wsl -l -v
```

To set WSL 2 as the distro version for the specific instance, run the command  `wsl --set-version <distro_name> 2` . Substitute the <distro_name> in my case is Ubuntu 

```
wsl --set-version Ubuntu 2
```

![Windows 10 WSL Distro Converted To WSL2](assets/images/posts/README/Windows-10-WSL-distro-converted-to-WSL2.png)

To convert a distro to WSL 1, issue the command `wsl --set-version <distro_name> 1`, e.g. for Ubuntu run: `wsl --set-version Ubuntu 1`.





## Step 2.  Pulling the data 

In this section we are going to get the data and convert to parquet.

Let us enter to the folder called **input**

inside there we get a  notebook called  `input-data.ipynb` 

we open it and we Select the Kernel  **Python 3.8 - Spark (ETL)**

![](assets/images/posts/README/1.jpg)

we select Run and then click on Run all cells

![](assets/images/posts/README/2.jpg)



The notebook will pull HMP accelerometer sensor data classification data set.

At the end of the notebook you will get 

![](assets/images/posts/README/3.jpg)



It was created inside the data folder a data.csv file

<img src="assets/images/posts/README/4.jpg" style="zoom:50%;" />

Great you have finished the second step.

**Troubleshooting**

Sometimes if you have lack of memory , you can get a java error.  A stupid way to solve this problem is **Restart Kernel and Run All Cells.**

<img src="assets/images/posts/README/trouble1.jpg" style="zoom:50%;" />



## Step 3. Reading  the `parquet` data

The next step is convert that csv file to parquet.

we open the notebook  `spark-csv-to-parquet.ipynb` inside the /transform/  folder and 

again we Select the Kernel  **Python 3.8 - Spark (ETL)** and run all cells

![](assets/images/posts/README/5.jpg)



now,  inside the data folder you have the .parquet file



![](assets/images/posts/README/4-16338584583421.jpg)



Let us now practice to do the opposite way, let us convert these parquet  files to csv. 

## Step 4. Conversion of `parquet`  to `CSV` format.

We are going to covert the `parquet` file to `CSV` format. 

Inside the transform folder open  the nootebook  `spark-parquet-to-csv.ipynb` and again we Select the Kernel  **Python 3.8 - Spark (ETL)** and run all cells. It will create a new folder called **data-new.csv**



![](assets/images/posts/README/7.jpg)



**Troubleshooting**

Sometimes if you have lack of memory , you can get a java error. To solve this problem is **Restart Kernel and Run All Cells.**





### Step 5. Loading the CSV file into a RDD Dataframe and Hyper Parameter tunning

In this section, we are going to import the CSV files into the RDD Dataframe and train a Random Forest model with a 80-20 training and test split  and get the Hyper Parameter tunning.

![](assets/images/posts/README/tuner.jpg)



## Step 6.   Machine Learning Model Creation



You open the `spark-train-lr.ipynb` and run all cells, you will create  model.xml file of your Machine Learning Model.



![](assets/images/posts/README/10.jpg)



### Step 8. Running Airflow locally

In order to create the Pipeline, we need a server with Airflow, in our project let us create a simple Airflow Locally by uysing Docker.

### Creating a personal access token in Git

You should create a personal access token to use in place of a password with the command line or with the API.  here are the [instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)

Once you have a token, you can enter it instead of your password when performing Git operations over HTTPS



### Create a repo in Github

To put your DAGs of airflow u on GitHub, you'll need to create a repository for it to live in. [here instructions](https://docs.github.com/en/get-started/quickstart/create-a-repo)





# Setting up Docker Compose

The first step is install [docker](https://docs.docker.com/desktop/windows/install/) 

![[](https://docs.docker.com/desktop/windows/install/)](assets/images/posts/README/down.jpg)

Docker containers can be ran in two ways: either in a bespoke capacity via the command line, or using a tool called [Docker Compose](https://docs.docker.com/compose/) that takes a yaml file which specifies which containers to run and how, and then does what's needed. 

go to the folder airflow-docker

To enable running of notebook pipelines on an existing Apache Airflow deployment

- Enable Git as DAG storage by customizing the [Git settings in `airflow.cfg`](https://github.com/apache/airflow/blob/6416d898060706787861ff8ecbc4363152a35f45/airflow/config_templates/default_airflow.cfg#L913).

First, we will start by creating DATA directory that will be used by Elyra to put DAG to github :

```
mkdir ./data
```

Next, we will create `./airflow` directory this one will hold files inside Airflow’s `AIRFLOW_HOME`:

```
mkdir ./airflow
```

```
cd airflow
```

In this step you will try to create new file called `airflow.cfg` inside folder AIRFLOW and add to it these two lines:

```
[kubernetes]
# Git credentials and repository for DAGs mounted via Git 
git_repo =https://github.com/Cloud-Data-Science/DAG
git_branch =main
```





- Install the [`airflow-notebook` Python package](https://github.com/elyra-ai/airflow-notebook) in the web-server, scheduler, and worker pods.



```
pip install airflow-notebook
```

we return back to the folder  airflow-docker

```
cd ..
```

and there is a  `docker-compose.yml` file that looks like this:

```
---
version: '3'
x-airflow-common:
  &airflow-common
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.0.1}
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-50000}"
  depends_on:
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  redis:
    image: redis:latest
    ports:
      - 6379:6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always

  airflow-worker:
    <<: *airflow-common
    command: celery worker
    restart: always

  airflow-init:
    <<: *airflow-common
    command: version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}

  flower:
    <<: *airflow-common
    command: celery flower
    ports:
      - 5555:5555
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

volumes:
  postgres-db-volume:
```



Docker containers are created based on Docker *images*, which hold the starting state for a container. 

Before starting Airflow for the **first time,** You need to prepare your environment, i.e. create the necessary files, directories and initialize the database.

To initialize the environment, run the following command:

```
docker-compose up airflow-init
```

After initialization is complete, you should see a message similar to that below:

![](assets/images/posts/README/creation.jpg)

The account created has the login `airflow` and the password `airflow`.

### **Run the Airflow**

You can start running the Airflow  container by running the command:

```
docker-compose up
```



![](assets/images/posts/README/web2.jpg)



You should notice that Airflow is suddenly a lot happier. You should also be able to connect to Airflow by visiting [localhost:8080](http://localhost:8080/) in your browser:



![](assets/images/posts/README/login.jpg)

and finally you got are in the terminal of airflow

![](assets/images/posts/README/example2.jpg)









## Configuring Apache Airflow on Kubernetes for use with Elyra

Pipelines in Elyra can be run locally in JupyterLab, or Apache Airflow to take advantage of shared resources that speed up processing of compute intensive tasks.

We need **Cloud Object Storage** endpoint.

I will create a   [Minio Server](https://min.io/download#/windows)  In windows

We open power shell

```
Invoke-WebRequest -Uri "https://dl.min.io/server/minio/release/windows-amd64/minio.exe" -OutFile "C:\minio.exe"
```

```
setx MINIO_ROOT_USER admin
```

```
setx MINIO_ROOT_PASSWORD password
```

I will use a external SSD in D drive

```
C:\minio.exe server D:\Data --console-address ":9001"
```

![](assets/images/posts/README/mini.jpg)



From here what we will use is the 

```
API: http://192.168.0.7:9000  http://172.21.32.1:9000  http://192.168.56.1:9000  http://127.0.0.1:9000  
```



Then  you can open the console [http://192.168.0.7:9001/login]( http://192.168.0.7:9001/login)

![](assets/images/posts/README/minio.jpg)



![](assets/images/posts/README/console.jpg)



we create bucket called `store`

![](assets/images/posts/README/store2.jpg)

if you need  ubuntu you can follow this [link](https://min.io/download#/linux)







## Step 7. Creation of the Pipeline

Creation of the Pipeline in Jupyter Lab

We open new terminal and we go to our folder

```
cd
```

```
conda activate etl
```

```
pyspark
```

Then we create a **New Apache Airflow Runtime**

![](assets/images/posts/README/runtime1.jpg)





then we Create a configuration with Name `Apache Airflow Local`

<img src="assets/images/posts/README/runtime2a.jpg" style="zoom:50%;" />



then we add our Apache Airflow endpoint





![](assets/images/posts/README/runtimeA2.jpg)



and finally our cloud storage object. This should be the URL address of your S3-compatible Object Storage. If running an Object Storage Service within a Kubernetes cluster (Minio), you can use the Kubernetes local DNS address. This setting is required.

We open a new teminal as administrator and we open the  hosts

```
C:\WINDOWS\system32>notepad c:\windows\system32\drivers\etc\hosts
```

at the end of the file  we add

```
# End of section
192.168.0.7 myminio.server
```

then you can check if works.

 You should also be able to connect to Console of Minio  by visiting [http://myminio.server:<port>/login](http://myminio.server/) in your browser where <port> is the port of you Minio Console Server.



and then we return back to our setup

![](assets/images/posts/README/minio2.jpg)







then **save and close**



![](assets/images/posts/README/Capture2a.JPG)

### Step 9. Runtime Images with Pyspark

We select the Runtime Image tab button

![](assets/images/posts/README/run1.jpg)





then we add new Runtime Image

<img src="assets/images/posts/README/run2.jpg" style="zoom:50%;" />



Like name we call  **Pyspark 3.02**  and the source: **ruslanmv/pyspark-runtime:3.0.2**with never policy then **save & close**

<img src="assets/images/posts/README/runtime-16342453275003.jpg" style="zoom:50%;" />

you will have the Runtime added 



<img src="assets/images/posts/README/added.jpg" style="zoom:50%;" />



### Step 10. Airflow editor

Let us create the following pipeline:

1. Pulling the data 
2. Reading  the `parquet` data
3. Conversion of `parquet`  to `CSV` format.
4. Loading the CSV file into a RDD Dataframe
5. Machine Learning Model Creation



We open the Pipeline editor

![](assets/images/posts/README/pipeline%20editor.jpg)

we create a new notebook



![](assets/images/posts/README/pipeline1.jpg)



then we select open properties

<img src="assets/images/posts/README/Capture3a.jpg" style="zoom:50%;" />



and we call the this node like `input` for the node and we select the filename the notebook 

`../input/input-data.ipynb` and we select the Runtime image **Pyspark 3.0.2**

<img src="assets/images/posts/README/capture6a.jpg" style="zoom:50%;" />



additionally you can select the amount of memory and CPUs

<img src="assets/images/posts/README/cpu.jpg" style="zoom:50%;" />



then we return back and  run

<img src="assets/images/posts/README/Capture4.JPG" alt="Capture5" style="zoom:50%;" />



then call the pipeline etl

<img src="assets/images/posts/README/Capture5.JPG" alt="Capture4" style="zoom:50%;" />

and click OK

<img src="assets/images/posts/README/Capture6.JPG" alt="Capture3" style="zoom:50%;" />

and  OK

<img src="assets/images/posts/README/Capture7.JPG" style="zoom:50%;" />

In the same way you can create the whole complete ETL



![](assets/images/posts/README/full-etl.jpg)



**Congratulations!** you have submitted your first Airflow application.
