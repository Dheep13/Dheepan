const fs = require('fs');

fs.readdir('./app', function(err, files){

    if (err) console.log('Error',err);
    else console.log('Result', files)

})