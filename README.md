# ETL and Machine Learning 

*How to create an ETL pipeline  with Machine Learning  by using Airflow and Spark*. 

We are going to build an ETL pipeline  by using **Airflow** with **Jupyter Lab**.  We will describe all the steps  to create the ETL.  

First we  will create a Python environment with all needed tools. Then we are going to create different notebooks of  the ETL, such as the extraction of the data, transformation of the data , creation of the Machine Learning Model and the deployment. 

## Contents

1. Setup of the Environment
2. Pulling the data 
3. Reading  the `parquet` data
4. Covert the `parquet` file to `CSV` format.
5. Loading the CSV file into a RDD Dataframe
6. Machine Learning Model Creation.
7. Training and test split.
8. Creation of the Pipeline
9. Machine Learning Deployment

##  Requirements

- Python 3.8
- NodeJS 16.11 
- Anaconda
- Git
- Desire to learn :)

## 1. Setup of the Environment

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
pip3 install jupyterlab-git==0.30.0 --user
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



Great, you have installed the **Pipeline Editors** that we will use. 



![](assets/images/posts/README/editor.jpg)



For additional information of the extensions visit the original source of Elyra [here]( https://elyra.readthedocs.io/en/latest/getting_started/installation.html#conda).

## 2.  Pulling the data 

In this section we are going to get the data and convert to parquet.

First we clone the repository that we will work,  for example `ETL-and-Machine-Learning`

inside there we get a  notebook called  `input-data.ipynb` 

we open and we Select the Kernel  **Python 3.8 - Spark (ETL)**

![](assets/images/posts/README/1.jpg)









## 3. Reading  the `parquet` data

Here we we are going to read the parquet data

we create a notebook  `spark-csv-to-parquet.ipynb`



## 4. Conversion of `parquet`  to `CSV` format.

We are going to covert the `parquet` file to `CSV` format.

let us create a notebook  `spark-parquet-to-csv.ipynb`



## 5. Loading the CSV file into a RDD Dataframe

We are going to import the CSV files into the RDD Dataframe





## 6. Machine Learning Model Creation

Here we are going to train a Random Forest model with different hyperparameter and report the best performing hyperparameter combinations.





### 7. Training and test split

Create a 80-20 training and test split with `seed=1`.



## 8. Creation of the Pipeline

Creation of the Pipeline in Jupyter Lab





## 9.Machine Learning Deployment

Deploy this model to Train a ML-Model on that data set

