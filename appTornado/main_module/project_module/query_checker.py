__author__ = 'SIBIA'

def query_checker(query_string):
    return_msg = []
    query_string = query_string.lower()
    words_list = query_string.split(' ')
    query_upper_list = make_operator_upper(words_list)
    operator = ['AND', 'OR', 'NOT', 'NEAR', 'IN']
    counter = 0
    while counter < len(query_upper_list):
        if (1 if query_upper_list[0] in operator else 0)==1:
            query_upper_list[0] = '<span style="color:red;">'+query_upper_list[0]+'</span>'
            return_msg.extend(['you can not start a query with an operator'])
        if (1 if query_upper_list[len(query_upper_list)-1] in operator else 0)==1:
            query_upper_list[len(query_upper_list)-1] = '<span style="color:red;">'+query_upper_list[len(query_upper_list)-1]+'</span>'
            return_msg.extend(['you can not end a query with an operator'])
        if (query_upper_list[counter] in operator) and (query_upper_list[counter-1] in operator):
            query_upper_list[counter]= '<span style="color:red;">'+query_upper_list[counter]+'</span>'
            return_msg.extend(['You can not use two operator in adjacent position'])
        counter+=1
    return_highlighted_query = ' '.join(query_upper_list)
    return return_highlighted_query, return_msg

def make_operator_upper(query_word_list):
    operator = ['and', 'or', 'not', 'near', 'in']
    index_list = [ query_word_list.index(op) if op in query_word_list else None for op in operator]
    upper_query_word_list = [word if query_word_list.index(word) not in index_list else word.upper() for word in query_word_list]
    return upper_query_word_list

#make_operator_upper(['messi', 'not', 'ronaldo', 'near', 'real', 'madrid', 'or', 'barca'])
query, msg = query_checker('and messi not in ronaldo or')
print query, msg