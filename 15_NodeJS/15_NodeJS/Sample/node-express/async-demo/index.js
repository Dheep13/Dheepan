//if we dont use call bacl, getUser will not return anything becuase there is a timeout defined which will delay the response

console.log('Before')

getUser(1,(user)=>{
    console.log('User', user)
});

console.log('After')


function getUser(id, callback){

    setTimeout(()=>{
        console.log('Reading from database....')
        callback ({id:id, gitusername:'Deepan'})
    }
    ,2000)

}

//promises equivalent for getUser *******very important*********

// function getUser(id){

// return new Promise((resolve, reject)=>{

//     setTimeout(()=>{
//         console.log('Reading from database....')
//         resolve ({id:id, gitusername:'Deepan'})
//     }
//     ,2000)
// })
// };


function getRepositories(username, callback) {
    setTimeout(() => {
      console.log('Calling GitHub API...');
      callback(['repo1', 'repo2', 'repo3']);
    }, 2000);
  }
  
  function getCommits(repo, callback) {
    setTimeout(() => {
      console.log('Calling GitHub API...');
      callback(['commit']);
    }, 2000);
  }