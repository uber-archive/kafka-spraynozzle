var cluster = require('cluster');
var http = require('http');

if (cluster.isMaster) {
    for(var i = 0; i < 8; i++) {
        cluster.fork();
    }
} else {
    http.createServer(function(req, res) {
        console.log('Got a request!');
        req.on('data', function(chunk) { console.log(chunk.toString()); });
        req.on('end', res.end.bind(res, '{}'));
    }).listen(11235);
}
