const https = require('https')

const data = JSON.stringify({
  todo: {
    "name": "morpheus",
    "job": "leader"
}
})

const options = {
  hostname: 'reqres.in',
  port: 443,
  path: '/api/users',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Content-Length': data.length
  }
}

const req = https.request(options, res => {
  console.log(`statusCode: ${res.statusCode}`)

  res.on('data', d => {
    process.stdout.write(d)
  })
})

req.on('error', error => {
  console.error(error)
})

req.write(data)
req.end()
