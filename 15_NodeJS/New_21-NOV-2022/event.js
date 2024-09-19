const Logger=require('./logger.js')
const logger = new Logger();


//Listener to an event
logger.on('messageLogged', (args)=>{
    console.log('Listening to event', args);
})


logger.log('message');