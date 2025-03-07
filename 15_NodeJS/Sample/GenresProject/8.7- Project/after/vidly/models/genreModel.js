const joi = require('Joi');
const mongoose = require('mongoose');


const Genre = mongoose.model('Genre', new mongoose.Schema({
  name: {
    type: String,
    required: true,
    minlength: 5,
    maxlength: 50
  }
}));


function validateGenre(genre) {
    const schema = {
      name: joi.string().min(3).required()
    };
  
    return joi.validate(genre, schema);
  }


  exports.Genre = Genre;
  exports.validateGenre = validateGenre;
  