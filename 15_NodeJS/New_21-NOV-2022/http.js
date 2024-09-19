const http = require('http');
console.log(http)
const server = http.createServer(function(req,res){
    if (req.url == '/') {
        res.write('Hello World 2');
        res.end();
    }
});


server.listen(3000);
console.log('Listening on port 3000...')
