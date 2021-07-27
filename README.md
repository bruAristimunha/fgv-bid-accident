<h1 align="center">
  <br> [Smartcities-FGV-BID] Real time Waze Accidents Probabilities
  <br>
</h1>
<p align="center">
   • <a href="">Mayuri Annerose Morais<sup>1</sup></a> 
   • <a href="https://www.linkedin.com/in/bruaristimunha/">Bruno Aristimunha<sup>1</sup></a> 
   • <a href="https://rycamargo.wixsite.com/home">Raphael Yokoingawa de  Camargo<sup>1</sup></a> 
</p>

> <sup>1</sup> Centro de Matemática, Computação e Cognição (CMCC), Universidade Federal do ABC (UFABC), Rua Arcturus, 03. Jardim Antares, São Bernardo do Campo, CEP 09606-070, SP, Brasil.

<p align="center">
<img src="https://raw.githubusercontent.com/bruAristimunha/fgv-bid-accident/master/dash_01.png"> 
</p>

<p align="center">
 Dashboard developed for proof of concept.
</p>



## This repository is the thematic implementation of the use of accident data in São Paulo, developed by the SmartCities-FGV project financed by the IDB. [[SmartCities Project]](https://smartcities-bigdata.fgv.br)


> **Abstract:** This project collects real-time Waze alerts and jams data and estimates a probability of an accident having victims. Before executing, we need to add the file "polygon.json" given by FGV, to the data folder. This file has sensitive information which can't be shared publicly. The result was saved to a CSV, which will be used in the front-end application. This CSV is updated every minute, registering the accident alerts that occurred in the last ten minutes.

<p align="center">
<img src="https://raw.githubusercontent.com/bruAristimunha/fgv-bid-accident/master/diagrama_relatorio.png"><br></br>Machine Learning Pipeline developed.</p>

--------------------

## Pre-requisite for Reproduction

Clone this repository

```shell
!git clone https://github.com/bruAristimunha/fgv-bid-accident
```

Install the necessary packages:

```shell
pip install -r requirements.txt
```

----

## Run aplication

Before executing, we need to add the file "polygon.json", given by FGV, to the data folder. This file has sensitive information which can't be shared publicly. With the python environment and all dependencies installed, run in the command line:

```
python waze-real-time.py &
```

With the back-end application running, after a time of alert collection and traffic jam (e.g. 30 min), you can run the front-end application in real time:

```
python front-end/dash_app-real-time.py  
```

If you have access to FGV's historical database, you can still run the historical base to the cities of São Paulo, Quito, Lima, Xalapa, and Montevideo. 

```
python front-end/dash_app.py 
```



