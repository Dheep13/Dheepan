// var fs = require('fs');
// var request = require('request');
// const options = {
//     method: "POST",
//     url: "https://excel-to-json-busy-badger-ck.cfapps.eu10.hana.ondemand.com/uploadfile",
//     port: 443,
//     headers: {
//         // "Authorization": "Basic " + auth,
//         "Content-Type": "multipart/form-data"
//     },
//     formData : {
//         "excel" : fs.createReadStream("scoping.xlsx")
//     }
// };

// request(options, function (err, res, body) {
//     if(err) console.log(err);
//     console.log(body);
// });
// const FormData = require('form-data');  
import FormData from 'form-data';   
const form = new FormData();
// let fs = require('fs');
import fs from 'fs';   
// let fetch = require('node-fetch');
import fetch from 'node-fetch';  

const buffer = fs.readFileSync('./foo.txt');
const fileName = 'foo.txt';

form.append('file', buffer, {
  contentType: 'text/plain',
  name: 'file',
  filename: fileName,
});

fetch('https://httpbin.org/post', { method: 'POST', body: form })
    .then(res => res.json())
    .then(json => console.log(json));