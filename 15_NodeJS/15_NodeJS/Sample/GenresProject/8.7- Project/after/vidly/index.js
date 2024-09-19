const mongoose = require('mongoose');
const genres = require('./routes/genres.js');
const express = require('express');
const app = express();

mongoose.connect('mongodb+srv://Deepan:nS2*B3gk_D4PGTX@cluster0.n49lg.mongodb.net/vidlydB')
// mongoose.connect('mongodb://localhost/vidlydB')

    .then(() => console.log('Connected to Mongoose DB..'))
    .catch(err => console.log('Connected to Mongoose DB..' , err) );

app.use(express.json());
app.use('/api/genres', genres);

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Listening on port ${port}...`));