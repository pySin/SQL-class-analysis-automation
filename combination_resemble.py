# Combination resemble
# A script that finds the closest combination to a target combination.
# The resembling combination must have at least 1 different class value
# for any of the class columns.

import mysql.connector
import re


# Create a table for the similarity data.
def create_table():

    send_query = '''
                CREATE TABLE IF NOT EXISTS protein_info.comb_resemble
                (Combination VARCHAR(30), entity VARCHAR(30),
                Behavior VARCHAR(30), Genotype VARCHAR(30),
                Treatment VARCHAR(30), AVG_comb FLOAT(8),
                MIN_comb FLOAT(8), MAX_comb FLOAT(8), AVG_diff FLOAT(8),
                MIN_dif FLOAT(8), MAX_dif FLOAT(8));
                 '''

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                               password = 'dance') # MySQL connection
    cursor = conn.cursor()
    cursor.execute(send_query)
    conn.commit()
    


# Get the names of the proteins placed in the first main column in
# the classes combination table.
def get_entities(table_name):

    send_query = '''
                SELECT DISTINCT entity FROM %s;
                 ''' % table_name

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                               password = 'dance') # MySQL connection
    cursor = conn.cursor()
    cursor.execute(send_query)
    entities = cursor.fetchall()
    entities = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in entities]
        # Remove the useless symbols from the results.  

    conn.commit()
    return entities


# Choose a protein name for the target combination to resemble.
def choose_entity(table_name):
    
    entities = get_entities('protein_info.protein_levels_52020_comb')
    entity = input('Choose an entity: '+str(entities)+' \n :')

    if entity not in entities:
        print('The value must of the entities above!')
        raise(ValueError)

    return entity


# Get the bahaviour names available.
def get_behaviours(table_name):

    send_query = '''
                SELECT DISTINCT Behavior FROM %s;
                 ''' % table_name

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                               password = 'dance') # MySQL connection
    cursor = conn.cursor()
    cursor.execute(send_query)
    behaviours = cursor.fetchall()
    behaviours = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in behaviours]

    conn.commit()

    return behaviours
    

# Choose a behaviour for the target combination to resemble.
def choose_behaviour(table_name):
    
    behaviours = get_behaviours(table_name)
    behaviour = input('Choose behaviour: '+str(behaviours)+'\n: ')

    if behaviour not in behaviours:
        print('You must choose from the list of Behaviours')
        raise(ValueError)

    return behaviour


# Get the Genotypes available.
def get_genotype(table_name):

    send_query = '''
                SELECT DISTINCT Genotype FROM %s;
                 ''' % table_name

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                               password = 'dance') # MySQL connection
    cursor = conn.cursor()
    cursor.execute(send_query)
    genotypes = cursor.fetchall()
    genotypes = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in genotypes]

    conn.commit()

    return genotypes
    

# Choose a Genotype for the target combination to resemble.
def choose_genotype(table_name):
    
    genotypes = get_genotype(table_name)
    genotype = input('Choose Genotype: '+str(genotypes)+'\n: ')

    if genotype not in genotypes:
        print('You must choose from the list of Genotypes')
        raise(ValueError)

    return genotype


# Get the Treatment names available.
def get_treatments(table_name):

    send_query = '''
                SELECT DISTINCT Treatment FROM %s;
                 ''' % table_name

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                               password = 'dance') # MySQL connection
    cursor = conn.cursor()
    cursor.execute(send_query)
    treatments = cursor.fetchall()
    treatments = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in treatments]

    conn.commit()

    return treatments
    

# Choose a treatment for the target combination to resemble.
def choose_treatment(table_name):
    
    treatments = get_treatments(table_name)
    treatment = input('Choose Treatment: '+str(treatments)+'\n: ')

    if treatment not in treatments:
        print('You must choose from the list of Genotypes')
        raise(ValueError)

    return treatment


# Get the class column names available.
# With this SQL script we exclude the columns which are not classes.
def diff_column(table_name):

    table_schema = str(table_name).split('.')[0]
    table_name = str(table_name).split('.')[1]

    send_query = '''
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = \'%s\'
                AND
                TABLE_NAME = \'%s\' AND
                COLUMN_NAME != 'entity' AND
                COLUMN_NAME != 'AVG' AND
                COLUMN_NAME != 'MAX' AND
                COLUMN_NAME != 'MIN';
                 ''' % (table_schema, table_name)

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                               password = 'dance') # MySQL connection

    cursor = conn.cursor()
    cursor.execute(send_query)
    col_names = cursor.fetchall()
    col_names = [re.sub(r'\(|\)|\'|,', '', str(x)) for x in col_names]

    conn.commit()
    return col_names


# Choose a column defined as class for the target combination to resemble.
def choose_diff_column(table_name):

    col_names = diff_column('protein_info.protein_levels_52020_comb')
    col_name = input('Choose differentiation column: '+str(col_names)+'\n')

    if col_name not in col_names:
        print('You must choose a column from the list!')
        raise(ValueError)

    return col_name

# Create MySQL query to fill in the data for the target class combiination.
# The other class combinations has to resemble this one.
def primary_combination(table_name, Entity, Treatment, Genotype, Behaviour):

    send_query = '''
                INSERT INTO protein_info.comb_resemble(Combination, entity,
                Behavior, Genotype, Treatment, AVG_comb, MIN_comb, MAX_comb,
                AVG_diff, MIN_dif, MAX_dif)
                
                SELECT 'main', entity, Behavior, Genotype, Treatment, AVG,
                MIN, MAX,
                
                (WITH f_sift AS(
                SELECT AVG, ABS(((SELECT AVG
                FROM %s
		WHERE entity = \'%s\' AND Treatment = \'%s\'
		AND Genotype = \'%s\' AND Behavior = \'%s\') 
                - AVG)) AS 'AVG_Difference' 
                FROM %s
                WHERE entity = \'%s\' AND Treatment != \'%s\'
                ) SELECT AVG_Difference FROM f_sift
                ORDER BY AVG_difference ASC
                LIMIT 1),

                (WITH f_sift AS(
                SELECT MIN, ABS(((SELECT MIN
                FROM %s
		WHERE entity = \'%s\' AND Treatment = \'%s\'
		AND Genotype = \'%s\' AND Behavior = \'%s\') 
                - MIN)) AS 'MIN_Difference' 
                FROM %s
                WHERE entity = \'%s\' AND Treatment != \'%s\'
                ) SELECT MIN_Difference FROM f_sift
                ORDER BY MIN ASC
                LIMIT 1),
 

                (WITH f_sift AS(
                SELECT MAX, ABS(((SELECT MAX
                FROM %s
		WHERE entity = \'%s\' AND Treatment = \'%s\'
		AND Genotype = \'%s\' AND Behavior = \'%s\') 
                - MAX)) AS 'MAX_Difference' 
                FROM %s
                WHERE entity = \'%s\' AND Treatment != \'%s\'
                ) SELECT MAX_Difference FROM f_sift
                ORDER BY MAX ASC
                LIMIT 1)
                
                
                FROM protein_info.protein_levels_52020_comb
                WHERE entity = \'%s\' AND Behavior = \'%s\'
                AND Genotype = \'%s\' AND Treatment = \'%s\'; 

                 ''' % (table_name, Entity, Treatment, Genotype, Behaviour,
                        table_name, Entity, Treatment,
                        table_name, Entity, Treatment, Genotype, Behaviour,
                        table_name, Entity, Treatment,
                        table_name, Entity, Treatment, Genotype, Behaviour,
                        table_name, Entity, Treatment,
                        Entity, Behaviour, Genotype, Treatment)

    print(send_query)

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance') # MySQL connection

    cursor = conn.cursor()
    cursor.execute(send_query)

    conn.commit()
    

# Crerate MySQL query to find the combination which resembles the main target
# combination the most.
def resemble_combination(table_name, Entity, Treatment, Genotype, Behaviour):

    send_query = '''
        INSERT INTO protein_info.comb_resemble(Combination, entity, Behavior,
                Genotype, Treatment, AVG_comb, MIN_comb, MAX_comb, AVG_diff,
                MIN_dif, MAX_dif)

                WITH f_sift AS(
                SELECT entity, Genotype, Treatment, Behavior, AVG, MIN, MAX,
                ABS(((SELECT AVG FROM protein_info.protein_levels_52020_comb
		WHERE entity = \'%s\' AND Treatment = \'%s\'
		AND Genotype = \'%s\' AND Behavior = \'%s\')
                - AVG)) AS 'AVG_Difference'
                FROM protein_info.protein_levels_52020_comb
                WHERE entity = \'%s\' AND Treatment != \'%s\'
                ) SELECT 'resemble', entity, Behavior, Genotype, Treatment,
                AVG, MIN, MAX, AVG_Difference,
                
                (WITH f_sift AS(
                SELECT MIN, ABS(((SELECT MIN FROM
                protein_info.protein_levels_52020_comb
		WHERE entity = \'%s\' AND Treatment = \'%s\'
		AND Genotype = \'%s\' AND Behavior = \'%s\') 
                - MIN)) AS 'MIN_Difference'
                FROM protein_info.protein_levels_52020_comb
                WHERE entity = \'%s\' AND Treatment != \'%s\'
                ) SELECT MIN_Difference FROM f_sift
                ORDER BY MIN ASC
                LIMIT 1), 

                (WITH f_sift AS(
                SELECT MAX, ABS(((SELECT MAX
                FROM protein_info.protein_levels_52020_comb
                WHERE entity = \'%s\' AND Treatment = \'%s\'
                AND Genotype = \'%s\' AND Behavior = \'%s\')
                - MAX)) AS 'MAX_Difference'
                FROM protein_info.protein_levels_52020_comb
                WHERE entity = \'%s\' AND Treatment != \'%s\'
                ) SELECT MAX_Difference FROM f_sift
                ORDER BY MAX ASC
                LIMIT 1) 

                FROM f_sift
                ORDER BY AVG_Difference ASC
                LIMIT 1;
                 ''' % (Entity, Treatment, Genotype, Behaviour, Entity,
                        Treatment, Entity, Treatment, Genotype, Behaviour,
                        Entity, Treatment, Entity, Treatment, Genotype,
                        Behaviour, Entity, Treatment)

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance') # MySQL connection

    cursor = conn.cursor()
    cursor.execute(send_query)
    conn.commit()
    

# Call all the functions. First determine each class value for each class
# column(input choose). Than run the queries to insert the data into a table.
def call_functions(table_name):

    create_table()

    Entity = choose_entity(table_name)
    Behaviour = choose_behaviour(table_name)
    Genotype = choose_genotype(table_name)
    Treatment = choose_treatment(table_name)
    Diff_column = choose_diff_column(table_name)
    Diff_column = [Diff_column, eval(Diff_column)]

    Behaviour = Behaviour.replace('/', '\/')
    
    primary_combination(table_name, Entity, Treatment, Genotype, Behaviour)

    resemble_combination(table_name, Entity, Treatment, Genotype, Behaviour)

    # print(Entity, Behaviour, Genotype, Treatment, Diff_column)


if __name__ == '__main__': 
    call_functions('protein_info.protein_levels_52020_comb')






