const mongoose = require('mongoose');
const express = require('express');
const app = express();


// mongoose.connect('mongodb://localhost:27017/')
mongoose.connect('mongodb+srv://Deepan:nS2*B3gk_D4PGTX@cluster0.n49lg.mongodb.net/playground')
    .then(() => console.log('Connected to Mongoose DB..'))
    .catch(err => console.log('Connected to Mongoose DB..' , err) );
    
const Author = mongoose.model('Author', new mongoose.Schema({
    name: String,
    bio: String,
    website: String

}));

//now create the class 

const Course = mongoose.model('Course', new mongoose.Schema({
    name: String
  }));


//now create an object of the above class 

async function createAuthor(name, bio, website) {
    const author = new Author ({
        name,
        bio,
        website
    
    });
    const result = await author.save()
    console.log(result)
};


async function createCourse(name,author) {
    const course = new Course ({
        name,
        author
    });
    const result = await course.save()
    console.log(result)
};


async function listCourses() {

    const courses = await Course
        .find()
        .select("name")
    console.log(courses)

};
// getCourses();

createAuthor('Deepan', 'My Bio', 'My Website')

// app.use(express.json());
// const port = process.env.PORT || 3000;
// app.listen(port, () => console.log(`Listening on port ${port}...`));