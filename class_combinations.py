# All possible class combinations

import entities_classes
import mysql.connector
import re
import random
import time


classes, attributes, dictionary = entities_classes.run_functions(
    'protein_data.protein_levels_2')
    # Get the class columns list, the non-class columns list and the
    # class column names with unique values dictionary from the
    # 'entities_classes' file 

def combinations_2_class(dictionary):
    # Create all the possible combinations between the classes when the classes
    # are 2

    nested_class = []
    for item in dictionary:
        nested_class.append(dictionary[item])
        # Create a list with the groups class values from the class columns.

    class_combinations = []
    
    for item in nested_class:
        if nested_class.index(item) == 1:
            break
        for item_1 in item:
            for item_2 in nested_class[nested_class.index(item)+1]:
                class_combinations.append([item_1, item_2])
        # Create a list with all the possible class combinations between the
        # class groups. 

    return class_combinations

# loop_dict(dictionary)


def combinations_3_class(dictionary):
    # Create all the possible combinations between the classes when the classes
    # are 3

    nested_class = []
    for item in dictionary:
        nested_class.append(dictionary[item])

    class_combinations = []
    
    for item in nested_class:
        if nested_class.index(item) == 1:
            break
        for item_1 in item:
            for item_2 in nested_class[nested_class.index(item)+1]:
                for item_3 in nested_class[nested_class.index(item)+2]:
                    class_combinations.append([item_1, item_2, item_3])

    return class_combinations


def combinations_4_class(dictionary):
    # Create all the possible combinations between the classes when the classes
    # are 4

    nested_class = []
    for item in dictionary:
        nested_class.append(dictionary[item])

    class_combinations = []
    
    for item in nested_class:
        if nested_class.index(item) == 1:
            break
        for item_1 in item:
            for item_2 in nested_class[nested_class.index(item)+1]:
                for item_3 in nested_class[nested_class.index(item)+2]:
                    for item_4 in nested_class[nested_class.index(item)+3]:
                        class_combinations.append([item_1, item_2, item_3,
                                                   item_4])

    return class_combinations


def combinations_5_class(dictionary):
    # Create all the possible combinations between the classes when the classes
    # are 5
    
    nested_class = []
    for item in dictionary:
        nested_class.append(dictionary[item])

    class_combinations = []
    
    for item in nested_class:
        if nested_class.index(item) == 1:
            break
        for item_1 in item:
            for item_2 in nested_class[nested_class.index(item)+1]:
                for item_3 in nested_class[nested_class.index(item)+2]:
                    for item_4 in nested_class[nested_class.index(item)+3]:
                        for item_5 in nested_class[nested_class.index(item)+4]:
                            class_combinations.append([item_1, item_2, item_3,
                                                   item_4, item_5])

    return class_combinations


def combinations_6_class(dictionary):
    # Create all the possible combinations between the classes when the classes
    # are 6
    
    nested_class = []
    for item in dictionary:
        nested_class.append(dictionary[item])

    class_combinations = []
    
    for item in nested_class:
        if nested_class.index(item) == 1:
            break
        for item_1 in item:
            for item_2 in nested_class[nested_class.index(item)+1]:
                for item_3 in nested_class[nested_class.index(item)+2]:
                    for item_4 in nested_class[nested_class.index(item)+3]:
                        for item_5 in nested_class[nested_class.index(item)+4]:
                            for item_6 in nested_class[nested_class.index(item)+5]:
                                class_combinations.append([item_1, item_2,
                                                           item_3, item_4,
                                                           item_5, item_6])

    return class_combinations


def combinations_7_class(dictionary):
    # Create all the possible combinations between the classes when the classes
    #  are 7

    nested_class = []
    for item in dictionary:
        nested_class.append(dictionary[item])

    class_combinations = []
    
    for item in nested_class:
        if nested_class.index(item) == 1:
            break
        for item_1 in item:
            for item_2 in nested_class[nested_class.index(item)+1]:
                for item_3 in nested_class[nested_class.index(item)+2]:
                    for item_4 in nested_class[nested_class.index(item)+3]:
                        for item_5 in nested_class[nested_class.index(item)+4]:
                            for item_6 in nested_class[nested_class.index(item)+5]:
                                for item_7 in nested_class[nested_class.index(item)+6]:
                                    class_combinations.append([item_1, item_2,
                                                               item_3, item_4,
                                                               item_5, item_6,
                                                               item_7])

    return class_combinations


def pick_function(classes):
    # Depending on the number of classes found in the table pick the proper
    # function. This file works with up to 7 classes.  

    len_classes = len(classes)

    if len_classes == 1:
        raise(ValueError)

    if len_classes == 2:
        combinations = combinations_2_class(dictionary)
    elif len_classes == 3:
        combinations = combinations_3_class(dictionary)
    elif len_classes == 4:
        combinations = combinations_4_class(dictionary)
    elif len_classes == 5:
        combinations = combinations_5_class(dictionary)
    elif len_classes == 6:
        combinations = combinations_6_class(dictionary)
    elif len_classes == 7:
        combinations = combinations_7_class(dictionary)

    return combinations


def compare_data_table(table_name, classes):
    # Create table for the combination data.

    classes = [x+' VARCHAR(100)' for x in classes]
    classes = str(classes)
    classes = re.sub(r'\[|\]|\'', '', classes) # Convert the class column
    # list to a sring ready for SQL execution. This string will be
    # incorporated in the bigger string 'send_query' below.
    
    # devide_name = table_name.split('.')
    new_table_name = str(table_name)+'_'+(str(time.localtime(time.time()).tm_mon)
            +str(time.localtime(time.time()).tm_year))+'_comb_2'
    # Create name for the new table.

    send_query = '''
            CREATE TABLE %s(
            entity VARCHAR(100),
            %s,
            AVG FLOAT(8),
            MIN FLOAT(8),
            MAX FLOAT(8)
            );
            ''' % (new_table_name, classes)
            # Create the table creating MySQL query.

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                    password = 'dance')

    cursor = conn.cursor()
    cursor.execute(send_query)
    conn.commit()
    return new_table_name


def populate_class_comp(table_name, entity, item, classes):
    # Populate the new table with data for all class combinations.

    new_table_name = str(table_name)+'_'+(str(time.localtime(time.time()).tm_mon)
            +str(time.localtime(time.time()).tm_year))+'_comb_2'
    # Recreate the new table name for the combination data.

    zipped_class = zip(classes, item)
    # Zip the class columns list with class values combination that was
    # created.

    zipped_list = []

    for i in zipped_class:
        zipped_list.append(i)
        # transfer the list pairs to a container list


    zipped_list = str(zipped_list)
    zipped_list = re.sub(r'\(\'|\]|\[', '', zipped_list)
    zipped_list = re.sub(r'\'\,', ' =', zipped_list)
    zipped_list = re.sub(r'\)', '', zipped_list)
    zipped_list = re.sub(r',', ' AND', zipped_list)
    # Transfer the list pairs to a string. Modify the string to resemble
    # part of MySQL query.

    item = str(item)
    item = re.sub(r'\[|\]', '', item)
    # Modify the combination of class values(item) to form MySQL query.

    classes = str(classes)
    classes = re.sub(r'\[|\]|\'', '', classes)
    # Modify a list to a proper string of class names.

    send_query = '''
                INSERT INTO %s(entity, %s, AVG, MIN, MAX)
                VALUES(\'%s\', %s,

                (SELECT AVG(%s) FROM %s
                WHERE
                %s),

                (SELECT MIN(%s) FROM %s
                WHERE
                %s),

                (SELECT MAX(%s) FROM %s
                WHERE
                %s)
                );
                 ''' % (new_table_name, classes, entity, item, entity,
                        table_name, zipped_list, entity, table_name,
                        zipped_list, entity, table_name, zipped_list)
        # Create the main MySQL query that populates the class combination
        # table with the class combinations and AVG, MIN and MAX values.
    

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance')

    cursor = conn.cursor()
    cursor.execute(send_query)
    conn.commit()


def call_functions(table_name, classes):

    class_combinations = pick_function(classes)
    # Create the class combinations tobe measured.

    compare_data_table(table_name, classes)
    # Create the table for the combination data.

    for entity in attributes:
        for item in class_combinations:
            populate_class_comp(table_name, entity, item, classes)
        # Create a loop to aquire the data objects(entities) and the class
        # combinations and run the function puting them properly into the
        # prepared table.

if __name__ == '__main__':        
    call_functions('protein_data.protein_levels_2', classes)




