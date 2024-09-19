const mongoose = require('mongoose');

//mongoose.connect returns a promise
mongoose.connect('mongodb+srv://Deepan:nS2*B3gk_D4PGTX@cluster0.n49lg.mongodb.net/test')
    .then(() => console.log('Connected to Mongoose DB..'))
    .catch(err => console.log('Connected to Mongoose DB..' , err) );


const courseSchema = new mongoose.Schema({
    name : String,
    author : String,
    tags : [String],
    date : {type: Date, default: Date.now},
    ispublished : Boolean

    }
);

//now create the class 

const Course = mongoose.model('Course', courseSchema)

async function removecourse(id){

    const result = await Course.deleteOne({_id:id});
    console.log(result)
    }
    removecourse('629fff40c4612434b89f775b');
