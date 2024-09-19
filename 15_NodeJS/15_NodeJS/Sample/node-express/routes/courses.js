const express = require('express');
const router = express.Router();
const validator = require('express-joi-validation').createValidator({})
//JOI is a class
const Joi = require('Joi');


const courses =[
{id: 1, name : 'course1'},
{id: 2, name : 'course2'},
{id: 3, name : 'course3'}
]


router.get('/api/courses/:id', (req,res) => {
  const course = courses.find( c => c.id === parseInt(req.params.id));
  if(!course) return res.status(404).send('The course with the give id was not found');
  return res.send(course);
});

router.post('/api/courses',(req,res) =>{

//    const result = validator.query(validation)
    // console.log(result);
    const course = {
        id : courses.length + 1 ,
        name : req.body.name
    };

    console.log(courses.length + 1);

    const result = validateCourse(req.body);
    if (result) {
            res.status(406);
            return res.json(
                 `Error in User Data : ${result.message}`
            );

    }
    else {
        courses.push(course);
        return res.send(course);
  };


});

router.put('/api/courses/:id',(req,res) =>{

    const course = courses.find( c => c.id === parseInt(req.params.id));
    if(!course) return res.status(404).send('The course with the give id was not found');
    const result = validateCourse(req.body);
    if (result) {
            res.status(406);
            return res.json(
                 `Error in User Data : ${result.message}`
            );

    }
    else {
        course.name=req.body.name
        return res.send(course);
  };


});

router.delete('/api/courses/:id',(req,res) =>{

    const course = courses.find( c => c.id === parseInt(req.params.id));
    if(!course) return res.status(404).send('The course with the give id was not found');
    const index = courses.indexOf(course);
//Delete
    courses.splice(index, 1);
//return the same course
    return res.send(course) 
});


function validateCourse(course){

    //validating client input using joi
  
      const validation = Joi.object({
          name: Joi.string().alphanum().min(3).max(25).trim(true).required()
          //id : Joi.number().required()
     });
  
         //this is object destructuring. Instead of using validation.validate(course).error, we are using {error}
         const { error } = validation.validate(course);
         return error;
     
  }


module.exports=router;