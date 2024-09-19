const EventEmitter=require('events');

class Logger extends EventEmitter {
    log(message){
    console.log('message');
    //emit event
    this.emit('messageLogged', {id:1, url:'https://'});
}}

module.exports = Logger;


