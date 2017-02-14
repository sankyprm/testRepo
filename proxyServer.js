var httpProxy = require('http-proxy');

var options = {
  // this list is processed from top to bottom, so '.*' will go to
  // '127.0.0.1:3000' if the Host header hasn't previously matched
  router : {
    'sibialabs.com': '127.0.0.1:8787',
    '.*': '127.0.0.1:3000'
  }
};

// bind to port 80 on the specified IP address
httpProxy.createServer(options).listen(80, '50.18.63.29');
