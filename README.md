# SQL-class-analysis-automation

## Introduction

Automate SQL analysis of different tables. The automation aims to find the columns that can be classified as  classes and then use these classes as the main point for analysis. The class columns with their class values can produce unique combinations which in the real world are the unique combinations of factors that influence the states of the objects(entities).

## Tasks

1. Find the class columns in any table and create all the possible class values combinations. Evaluate the rest of the columns(non-class columns) according to the class combinations to find the most important factor(factors) influencing the results in the non-class columns(entities). 

2. A script that finds the closest combination to a target combination. The resembling combination must have at least 1 different class value for any of the class columns(combination_resemble.py).

3. Find the sum of the difference for all the proteins from one combination with all other combinations from 'protein_levels_comb.csv'. The original data comes from https://www.kaggle.com/ruslankl/mice-protein-expression. Every protein from the 77 examined has different concentration value for every unique combination. The differences of all 77 proteins are summed to result in an overall difference('sum_comb_resemble.py'). 

## Technologies used:

* MySQL Workbench
* Python - mysql.connector, re, random, time

## Setup

1. Install MySQL DMS.
2. Install mysql.connector library to your Python distribution
3. Change the Hostname, Username and Password in the scripts where the connections are created(conn = mysql.connector.connect(host = 'localhost', user = 'root', password = 'dance').
4. Find a table to analyze. I'm using a table from here: https://www.kaggle.com/ruslankl/mice-protein-expression. Before using this table the column 'class' have to be removed, because it is some preliminary analysis that isn't needed. The data objects that are being analyzed should be with numeric values.


