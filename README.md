# SQL-class-analysis-automation

## Introduction

Automate SQL analysis of different tables. The automation aims to find the columns that can be classified as  classes and then use these classes as the main point for analysis. The class columns with their class values can produce unique combinations which in the real world are the unique combinations of factors that influence the states of the objects(entities).

## Tasks

1. Find the class columns in any table and create all the possible class values combinations. Evaluate the rest of the columns(non-class columns) according to the class combinations to find the most important factor(factors) influencing the results in the non-class columns(entities). 

## Technologies used:

* MySQL Workbench
* Python - mysql.connector, re, random, time

## Setup

1. Install MySQL DMS.
2. Install mysql.connector library to your Python distribution
3. Change the Hostname, Username and Password in the scripts where the connections are created(conn = mysql.connector.connect(host = 'localhost', user = 'root', password = 'dance').
4. Find a table to analyze. I'm using a table from here: https://www.kaggle.com/ruslankl/mice-protein-expression. Before using this table the column 'class' have to be removed, because it is some preliminary analysis that isn't needed. The data objects that are being analyzed shold be with numeric values.


