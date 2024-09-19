
const os = require('os');
var totalMemory = os.totalmem();
var availableMemory = os.freemem()
console.log(`Total memory is: ${totalMemory}`);
console.log(`Available memory is: ${availableMemory}`);