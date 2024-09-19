const mongoose = require('mongoose');

//mongoose.connect returns a promise
mongoose.connect('mongodb+srv://Deepan:nS2*B3gk_D4PGTX@cluster0.n49lg.mongodb.net/test')
    .then(() => console.log('Connected to Mongoose DB..'))
    .catch(err => console.log('Connected to Mongoose DB..' , err) );

const courseSchema = new mongoose.Schema ({
    name : {type: String,
         required : true},
    author : String,
    tags : {
        type: Array,
        validate: {
            //async validator
            // isAsync : true,
            validator : function (v){
                return new Promise( (resolve, reject) => {
                    setTimeout(() => {
                        //do some async work
                        const result = v && v.length > 0;
                        resolve (result);
                    }, 4000);
                })

            },
            message : 'A course should atleast have one tag'
        }
    },
    date : {type: Date, default: Date.now},
    ispublished : Boolean,
    price : {
        type: Number,
        required : function () {return this.ispublished;},
        min : 10,
        max : 100
    }
    }
);

//now create the class 

const Course = mongoose.model('Course', courseSchema)

//now create an object of the above class 


async function createCourse() {
    const course = new Course ({

        name : 'Test Course',
        author : 'Jeevitha',
        tags :[],
        ispublished : true,
        price :20
    
    })

    try {
        const result = await course.save()
        console.log(result)
        
    } catch (err) {
        console.log('Failed with error : ' + err)
        
    }
   
};

createCourse();


// async function getCourses() {

//     const courses = await Course
//         // .find({tags : 'CreateServer'})
//          // .find({tags : /^Create/'}) //starts with
//          // .find({tags : /Create$/i'}) //ends with
//          // .find({tags : /.*Create.*/i'}) //contains
//         // .select({name:1, tags:1})
//         .find()
//         .or([{tags:'CreateServer'},{name: 'Node.js Course'}])
//         //.count()
//     console.log(courses)

// };
// getCourses();


//To import json array
// mongoimport --uri 
// mongodb+srv://<USERNAME>:<PASSWORD>@<CLUSTER_NAME>/<DATABASE> --collection <COLLECTION> --type json --file <FILENAME>
//'mongodb+srv://Deepan:nS2*B3gk_D4PGTX@cluster0.n49lg.mongodb.net/mongo-exercises --collection courses --type json --file exercise-data.json