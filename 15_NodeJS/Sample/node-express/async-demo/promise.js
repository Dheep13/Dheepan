const p = new Promise((resolve, reject)=>{
    //kickoff some async work
    setTimeout(()=>{

        // resolve(1);
            reject(new Error('message'));
    }, 2000);
    

})

p

    .then(result => console.log('Result', result))
    .catch(err => console.log('Error', err.message));

// p.catch(result => console.log('Result', result))
