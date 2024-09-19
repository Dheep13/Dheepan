const express = require('express');
const router = express.Router();
const validator = require('express-joi-validation').createValidator({})
//JOI is a class
const Joi = require('Joi');


router.get('all', (req,res) => {
    return res.send(courses);
});

router.get('', (req,res) => {
    
    res.render('index',{title:'My express App', message:'Hello'});
    //return res.send('Hello World , hello deepan');
});

module.exports = router;