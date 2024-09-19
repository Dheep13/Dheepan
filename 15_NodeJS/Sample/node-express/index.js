const express = require('express');
const { not } = require('Joi');
const config = require('config');
const debug = require('debug')('app');

var helmet = require('helmet');
const log = require('./middleware/logger');

const app = express();
const home=require('./routes/home');
const courses=require('./routes/courses');


//process is a global object 
console.log(`NODE_ENV :${process.env.NODE_ENV}`);
console.log(`app :${app.get.env}`);

debug(`NODE_ENV :${process.env.NODE_ENV}`);

app.use(express.json());
app.use(express.urlencoded({extended : true}));
//to serve staticc content from the route of the site
app.use(express.static('public'));
app.use(log);
app.use(helmet());
app.set('view engine', 'pug');// this is to load the pug module
app.set('views', './views');//default path for pug
app.use('/', home);
app.use('/api/courses', courses);


console.log('Application name is ' + config.get('name'));
console.log('Mail server is ' + config.get('mail.host'));
console.log('Password is ' + config.get('mail.password'));
debug('Password is ' + config.get('mail.password'));


const port = process.env.PORT || 7000
app.listen(port, ()=> console.log(`Listening to port ${port}}...`));
