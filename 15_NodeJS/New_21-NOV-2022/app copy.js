// function sayHello(name){
//     console.log('Hello ' +name);
// }

// sayHello('Deepan');

const EventEmitter = require('events');
//EventEmitter is a class
// const emitter = new EventEmitter();

const os = require('os')
const fs = require('fs')
const log = require('./logger');
const Logger = require('./logger');


var totalmemory=os.totalmem
var freememomry=os.freemem
const logger = new Logger();


// const files = fs.readdirSync('./')
// const nonysncfiles = fs.readdir('./',function(err,files){
//     if (err) console.log('Error', err);
//     else console.log('Result', files);
// });

//Lets first register a listner
logger.on('messageLogged',(arg) => {
        console.log('Listener Called', arg)
    } );


logger.log('message');
// console.log('Total memory is ' + totalmemory);
// console.log(`Total memory is ${totalmemory}`);
// console.log(files);
// console.log(nonysncfiles);




const os = require('os');
var totalMemory = os.totalmem();
console.log(`Total memory is: ${totalMemory}`);