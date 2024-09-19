// const { func } = require('assert-plus');
const EventEmitter = require('events');
//EventEmitter is a class
const emitter = new EventEmitter();
//emitter is an object/instance of class EventEmitter

var url = 'http://mylogger.io/log';

//L in Logger is uppoer case - pascal class
// function inside a class is called a method. Notice that the keyword function has removed when inside a class for log
//if we want this logger class to have all the capabilites of the EventEmitter we will need to use extends
// because the class has all the capabilites of event emitter we can use this. to use any of EventEmitters capabilities for example this.emit 
class Logger extends EventEmitter {
    log (message) {
        //send an http message
        console.log(message)  ;
        //raise an event
      this.emit('messageLogged',{ id: 1, url : 'http://helloworld.com'})
      //--->{ id: 1, url : 'http://helloworld.com'} this is an object
      };
      

};


module.exports = Logger;


