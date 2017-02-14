__author__ = 'SIBIA'

import tornado.web
import tornado.gen
import tornado.httpclient
import json
import re

class QueryChecker(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        self.status = 0
        result  = yield tornado.gen.Task(self.query_checker)
        self.write(result)
        self.finish()
    def query_checker(self, callback):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        self.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        query_string = (self.get_argument('query')).encode('utf-8')
        query_string = query_string.decode().encode('utf-8')
        print query_string
        return_msg = []
        query_string = query_string.lower()
        query_string_formated = query_string.replace('"', "'")


        callback(self.error_calculator(query_string_formated))

    def make_operator_upper(self, query_word_list):
        operator = ['and', 'or', 'not', 'near', 'in']
        index_list = [ query_word_list.index(op) if op in query_word_list else None for op in operator]
        upper_query_word_list = [word if query_word_list.index(word) not in index_list else word.upper() for word in query_word_list]
        return upper_query_word_list

    def error_calculator(self, query_string):
        pointer_list = self.find("'", query_string)
        print "pointer_list", len(pointer_list)
        if (len(pointer_list)%2) is not 0 and len(pointer_list)!=0:
            self.status=1
            return {'query':query_string[:(pointer_list[len(pointer_list)-2])+1]+'<span style="color:red;"  title="There is an error in quoting the string" data-placement="bottom" data-toggle="tooltip">'+query_string[(pointer_list[len(pointer_list)-2])+1:]+'</span>', 'status':self.status}
        elif len(pointer_list)==0:
            self.status=1
            return {'query':'<span style="color:red;"  title="Please quote your keywords" data-placement="bottom" data-toggle="tooltip">'+query_string+'</span>', 'status':self.status}
        else:
            return self.find_wrong_in_operator_use(pointer_list, query_string)
    def find_wrong_in_operator_use(self, pointer_list, query_string):
        query_block_list = [query_string[:pointer_list[0]]] if query_string[:pointer_list[0]] is not '' else []
        query_quote_block_list = [query_string[pointer_list[0]:pointer_list[1]+1]]
        while_counter = 2
        while while_counter<len(pointer_list):
            query_block_list.append(query_string[pointer_list[while_counter-1]+1:pointer_list[while_counter]])
            query_quote_block_list.append(query_string[pointer_list[while_counter]:pointer_list[while_counter+1]+1])
            while_counter+=2
        last_block_with_tagged_error = []
        query_block_last = query_string[pointer_list[len(pointer_list)-1]+1:].split(" ")
        print 'query', query_block_last
        if len(query_block_last)>1:
            self.status=1
            last_block_with_tagged_error = '<span style="color:red;"  title="you can not end a query with an operator" data-placement="bottom" data-toggle="tooltip">'+(' '.join(query_block_last))+'</span>'
        print query_block_list, "===", query_quote_block_list
        print 'query block last===>>>', last_block_with_tagged_error
        error_tagged_list = [self.return_error_tagged_string(each_block) for each_block in query_block_list]
        error_tagged_list.append(last_block_with_tagged_error)
        print "error====>>", error_tagged_list
        if pointer_list[0] is not 0:
            print 'in if'
            return {'query':self.add_two_list(error_tagged_list, query_quote_block_list), 'status':self.status}
        else:
            print 'in else'
            return {'query':self.add_two_list(query_quote_block_list, error_tagged_list), 'status':self.status}



    def return_error_tagged_string(self, split_initial_block):
        split_initial_block = split_initial_block.upper()
        split_initial_block = split_initial_block.split(" ")
        print 'initial_block--->>>>', split_initial_block
        operator = ['AND', 'OR', 'NOT', 'NEAR', 'IN']
        operator_with_space = ['AND', 'OR', 'NOT', 'NEAR', 'IN', ' ', '']
        if split_initial_block[0] in operator:
            self.status = 1
            split_initial_block[0]= '<span style="color:red;"  title="you can not start a query with an operator" data-placement="bottom" data-toggle="tooltip">'+split_initial_block[0]+'</span>'
        if split_initial_block[0] not in operator_with_space:
            self.status = 1
            split_initial_block[0]= '<span style="color:red;"  title="Please use keyword inside quote" data-placement="bottom" data-toggle="tooltip">'+split_initial_block[0]+'</span>'
        counter = 1
        while counter < len(split_initial_block):
            if (split_initial_block[counter] in operator) and (split_initial_block[counter-1] in operator):
                split_initial_block[counter]= '<span style="color:red;"  title="You can not use two operator in adjacent position" data-placement="bottom" data-toggle="tooltip">'+split_initial_block[counter]+'</span>'
                self.status = 1
            if (split_initial_block[counter] not in operator_with_space):
                split_initial_block[counter]= '<span style="color:red;"  title="Please use keyword inside quote" data-placement="bottom" data-toggle="tooltip">'+split_initial_block[counter]+'</span>'
                self.status = 1
            counter+=1
        return ' '.join(split_initial_block)

    def find(self, ch, s):
        return [m.start() for m in re.finditer(ch, s)]

    def add_two_list(self, first, second):
        final_string = ''
        counter = 0
        while counter<len(first):
            try:
                final_string = final_string+first[counter]
            except:
                pass
            try:
                final_string = final_string+second[counter]
            except:
                pass
            counter+=1
        return final_string
if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/querychecker", QueryChecker)
    ],debug=True)
    application.listen(port=5000)
    print("Listenning on port 5000...")
    tornado.ioloop.IOLoop.instance().start()