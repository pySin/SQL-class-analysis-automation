# Automate Table Analysis

# Produce a list with the objects(entities) to analyze. Also produce
# attributes list through which the objects are analyzed.

import mysql.connector
import re
import random
import time


def get_col_names(table_name):
    # Get the table columns names. A string has to be constructed that looks
    # exactly like SQL query. This query is then sent to the MySQL server and
    # the result is assigned to a variable with fetchall() function.
    # An absolute name of the table is needed. From this name the database and
    # table names can be obtained. 

    database = table_name.split('.')[0] # Get database name.
    table = table_name.split('.')[1] # Get table name.
    
    get_c_names = ''' 
                  SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = \'%s\'
                  AND
                  TABLE_NAME = \'%s\';
                  ''' % (database, table) # Build the SQL query.
    
    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance') # Create MySQL connection.
    cursor = conn.cursor()
    cursor.execute(get_c_names)
    col_names = cursor.fetchall()

    p_names = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in col_names]
    # Transform the list from the fetchall() function to simple list of strings.
    

    conn.commit() # Send the query to MySQL
    # print('All columns:', p_names)
    return p_names

def table_create(table_name):
    # Build a table creating SQL query. This table will contain the results for
    # each column(class column or non class column)

    new_table_name = str(table_name)+'_'+(str(time.localtime(time.time()).tm_mon)
                                    +str(time.localtime(time.time()).tm_year))
    # Create name for the new table.
    
    mysql_send = '''
                CREATE TABLE %s(
                `column` VARCHAR(50),
                is_it_class VARCHAR(20),
                unique_percentage FLOAT
                ); 
                 ''' % new_table_name

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance')
    cursor = conn.cursor()     # There are no results to fetch here because all
    cursor.execute(mysql_send) # the changes are made on the server side.
    conn.commit()


def insert_found_classes(table_name, column_name):
    # Build SQL query to insert data into the new table. Here besides the
    # table name, column name is needed as well. This function fills data
    # one row per run.

    new_table_name = str(table_name)+'_'+(str(time.localtime(time.time()).tm_mon)
                                +str(time.localtime(time.time()).tm_year))
    # Recreate the name of the new table.

    mysql_insert = '''
        INSERT INTO %s(`column`, is_it_class, unique_percentage)
        SELECT \'%s\', (CASE
        WHEN
        ((WITH selection AS (SELECT DISTINCT %s FROM %s) SELECT COUNT(%s)
        FROM selection) / 
        (SELECT COUNT(%s) FROM %s)*100) < 20
        THEN 'class'
        ELSE 'Not a class'
        END) AS 'is_it_class', 
        ((WITH selection_2 AS (SELECT DISTINCT %s FROM %s) SELECT COUNT(%s)
        FROM selection_2) / 
        (SELECT COUNT(%s) FROM %s)*100) AS unique_percentage;
                  ''' % (new_table_name, column_name, column_name, table_name,
                         column_name, column_name, table_name, column_name,
                         table_name, column_name, column_name, table_name)
    # This query uses some subqueries to calculate which columns are less than
    # 20 percent unique. 

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance')
    cursor = conn.cursor()
    cursor.execute(mysql_insert)

    conn.commit()


def class_insert(table_name):
    # Insert the result values('class', 'not a class') with the
    # 'insert_found_classes() function'. Each loop runs the function and
    # evaluates 1 column as 'class' or 'not a class'.

    col_names = get_col_names(table_name)
    # Get the column names from the main table.

    for item in col_names:
        insert_found_classes(table_name, item)
        # Ren the 'insert_found_classes()' function for each column name.


def get_class_columns(table_name):
    # The new created table('protein_data.protein_levels_class')
    # has the class columns and we just need to extract them. The created list
    # will be used to extract the different classes from each class column.

    new_table_name = str(table_name)+'_'+(str(time.localtime(time.time()).tm_mon)
                                +str(time.localtime(time.time()).tm_year))
    # Recreate the name of the new table.

    mysql_get_class_columns = '''
                SELECT `column` FROM %s
                WHERE is_it_class = 'class';
                ''' % new_table_name
                # Extract the class values from the class tables.
    
    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                    password = 'dance')

    cursor = conn.cursor()
    cursor.execute(mysql_get_class_columns)
    classes = cursor.fetchall()
    conn.commit()

    classes = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in classes]
    # Transform the list from the fetchall() function to simple list of strings.
    
    return classes


def get_non_class_columns(table_name):
    # The new created table('protein_data.protein_levels_class')
    # has the class columns and we just need to extract them. The created list
    # will be used to extract the different columns which are not classes.

    new_table_name = str(table_name)+'_'+(str(time.localtime(time.time()).tm_mon)
                            +str(time.localtime(time.time()).tm_year))    
    # Recreate the name of the new table.

    mysql_get_class_columns = '''
                SELECT `column` FROM %s
                WHERE is_it_class = 'Not a class';
                ''' % new_table_name
    
    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                    password = 'dance')

    cursor = conn.cursor()
    cursor.execute(mysql_get_class_columns)
    not_classes = cursor.fetchall()
    conn.commit()

    not_classes = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in not_classes]
    return not_classes


def clear_non_class(table_name):
    # Some of the non-class columns have non-numeric data types and must be
    # removed before the analysis.
    
    database = table_name.split('.')[0] # Get database name by splitting the
                                        # whole name.
    table = table_name.split('.')[1] # Get table name.
    
    get_c_names = ''' 
                  SELECT COLUMN_NAME, DATA_TYPE
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = \'%s\'
                  AND
                  TABLE_NAME = \'%s\';
                  ''' % (database, table) # Build SQL query to extract the
                  # non-class names with their data types.
    
    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance') # MySQL connection
    cursor = conn.cursor()
    cursor.execute(get_c_names)
    col_names = cursor.fetchall()

    p_names = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in col_names]
    # Transform the list from the fetchall() function to a simple list of strings.
    
    conn.commit() # Send the query to MySQL
    
    get_non_col = get_non_class_columns(table_name)
    # Get the non_class columns.
    
    p_names = [x for x in p_names if x.split(' ')[0] in get_non_col]
    # From the new list, containing column names with data types, keep those
    # names that are in the 'non-class' column name list.

    accepted_types = ['tinyint', 'smallint', 'int', 'float', 'decimal',
                      'double']
    # Create a list with the acceptable data types. Columns has to be numeric
    # to allow calculations.

    for name in p_names:
        name = name.split(' ')
        if name[1] not in accepted_types:
            get_non_col.remove(name[0])
        else:
            pass
        # Check for non-accepted data types. If such datatype of a column is
        # found the column name is removed from tha main non-class column list
        # 'get_non_col'

    return get_non_col


def class_values(column, table_name):
    # Extract the different classes from each class column.

    mysql_class_values = '''
                    SELECT DISTINCT %s FROM %s;
                         ''' % (column, table_name) # Select each unique value
                    # from every class column. 

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                    password = 'dance')

    cursor = conn.cursor()
    cursor.execute(mysql_class_values)
    class_cols = cursor.fetchall()
    conn.commit()

    class_cols = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in class_cols]
    
    return class_cols


def classes_dict(table_name):
    # Create a dictionary with key the column name and values the different
    # classes in each class column.

    class_dictionary = {}
    
    class_cols = get_class_columns(table_name)
    for item in class_cols:
        class_dictionary[item] = class_values(item, table_name)

    return class_dictionary


def run_functions(table_name):
    # Run the class column creting functions and the non-class column lists creating functions.
    
    table_create(table_name)
    class_insert(table_name)
    class_col = get_class_columns(table_name)    
    non_class_col = clear_non_class(table_name)
    class_dictionary = classes_dict(table_name)

    return class_col, non_class_col, class_dictionary
