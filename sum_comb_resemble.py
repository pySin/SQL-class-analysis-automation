# Find the smallest difference for a class combination from another class
# combination analyzing protein concentration levels.
# 'protein_level_comb.csv' file is used(transfer the data to MySQL).

import mysql.connector


# Pick the main class combination to be compared with the rest of the
# protein combinations.
def pick_class_combination():

    # The classes with their values.
    Behavior = ['C/S', 'S/C']
    Genotype = ['Control', 'Ts65Dn']
    Treatment = ['Saline', 'Memantine']

    # Ask the user to pick Behaviour value.
    pick_behaviour = input('Choose bahaviour: '+str(Behavior)+':')
    if pick_behaviour not in Behavior:
        print('You must write a value from the given options!')
        raise(ValueError)

    # Ask the user to pick Genotypre value.
    pick_genotype = input('Choose genotype:'+str(Genotype)+':')
    if pick_genotype not in Genotype:
        print('You must write a value from the given options!')
        raise(ValueError)

    # Ask the user to pick Treatment value.
    pick_treatment = input('Choose treatment:'+str(Treatment)+':')
    if pick_treatment not in Treatment:
        print('You must write a value from the given options!')
        raise(ValueError)

    main_combination = [pick_behaviour, pick_genotype, pick_treatment]
    return main_combination
    

# Fetch all distinct class combinations from MySQL table.
def get_all_class_combinations():

    send_query = '''
                SELECT DISTINCT Behavior, Genotype, Treatment
                FROM protein_info.protein_levels_comb;
                 '''

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance')
    cursor = conn.cursor()
    cursor.execute(send_query)
    combinations = cursor.fetchall()

    conn.commit()
    return combinations    


# Remove the main combination that will be compared to all the others.
def remove_main():
    main_combination = pick_class_combination()
    resemble_combinations = get_all_class_combinations()

    for item in resemble_combinations:
        if item == tuple(main_combination):
            resemble_combinations.remove(item)

    #print(other_combs)
    return main_combination, resemble_combinations
    

# Create SQL query to find the overall difference between the main combination
# and each other combination.
def summed_similarity(main_combination, combination):

    # Get the class values for the main combination.
    Behavior = main_combination[0]
    Genotype = main_combination[1]
    Treatment = main_combination[2]

    # Get the class values for the secondary combination.
    Behavior_2 = combination[0]
    Genotype_2 = combination[1]
    Treatment_2 = combination[2]


    print('Main: ', main_combination)
    print('Treatmnet:', Treatment)
    
    print('Behaviour_2:', Behavior_2)
    print('Genotype_2:', Genotype_2)
    print('Treatment_2:', Treatment_2)


    # Insert the comparisson values with this MySQL query.
    send_query = '''
                INSERT INTO protein_info.protein_summed_difference(Behavior,
                Genotype, Treatment, VS, Behavior_2, Genotype_2, Treatment_2,
                Sum_difference)

                WITH sift AS(
                SELECT Behavior, Genotype, Treatment, AVG,
                \'-VERSUS-\' AS \'VS\', \'%s\',
                \'%s\', \'%s\', 
                
                ABS(AVG 
                - (SELECT AVG FROM protein_info.protein_levels_comb
                WHERE Behavior = \'%s\' AND Genotype = \'%s\'
                AND Treatment = \'%s\' AND entity = ppc.entity)
                ) AVG_Difference
                
                FROM protein_info.protein_levels_comb AS ppc
                WHERE Behavior = \'%s\' AND Genotype = \'%s\'
                AND Treatment = \'%s\')
                SELECT Behavior, Genotype, Treatment,
                '-VERSUS-' AS 'VS',
                \'%s\', \'%s\', \'%s\',
                SUM(AVG_Difference) AS 'Sum_difference' FROM sift;
                 ''' % (Behavior_2, Genotype_2, Treatment_2, Behavior_2,
                        Genotype_2, Treatment_2, Behavior, Genotype, Treatment,
                        Behavior_2, Genotype_2, Treatment_2)

    conn = mysql.connector.connect(host = 'localhost', user = 'root',
                                   password = 'dance')
    cursor = conn.cursor()
    cursor.execute(send_query)
    conn.commit()

    print(send_query)


# Call all the functions.
def call_functions():

    m_s_combinations = remove_main()
    main_combination = m_s_combinations[0]
    resemble_combinations = m_s_combinations[1]

    for combination in resemble_combinations:
        summed_similarity(main_combination, combination)

if __name__ == '__main__':
    call_functions()
    











