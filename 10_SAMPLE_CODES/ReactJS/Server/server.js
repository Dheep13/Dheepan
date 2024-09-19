const express = require('express') 
const cors = require('cors') 
const axios = require('axios').default 
const _ = require('lodash')
const config = require('./config')

const app = express() 
app.use(cors()) 
const port = 3009

app.get('/ibm/token', async (req, res) => 
{ try {
     const formdata = new URLSearchParams({
grant_type: 'urn:ibm:params: auth:grant-type: apikey',
apikęy: config.ibm. apikey,
})
const response = await axios.post('https://iam.cloud.ibm.com/identity/token', formdata)
const access_token = _.get (response, 'data.access_token', '')
res.status(200).json({
access_token,
})
} catch (error) {
     res.status(500).json({
error: error.message,
     })
}
})

app.listen (port, () => {
console.log('STT Demo App listening on port ${port}')
})
