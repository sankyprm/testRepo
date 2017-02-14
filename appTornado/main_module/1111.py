__author__ = 'sibia-10'

import tornado.web
import tornado.ioloop
import io
import os

class TestHandler(tornado.web.RequestHandler):

    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        self.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")

        path = '/home/ubuntu/sociabyte/sociabyte_app/public/upload/schedule_pics'
        fileinfo = self.request.files['datafile'][0]

        fbody = (fileinfo['body'])
        print(fbody)
        os.chdir(path)
        output_file = open("pic.png", 'w')
        output_file.write(fbody)
        form = self.get_argument('userid')
        print(form)
        self.finish('Done')




if __name__ == '__main__':

    # create tornado application with the aforesaid handlers
    application = tornado.web.Application([
        (r"/ajaxfetch", TestHandler),
            ])
    application.listen(8888)
    print("Listenning on 8888...")
    tornado.ioloop.IOLoop.instance().start()