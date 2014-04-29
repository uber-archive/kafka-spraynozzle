var http = require('http');

http.createServer(function(req, res) {
    console.log('Got a request!');
    req.on('data', function(chunk) { console.log(chunk.toString()); });
    req.on('end', res.end.bind(res, '{}'));
}).listen(11235);
