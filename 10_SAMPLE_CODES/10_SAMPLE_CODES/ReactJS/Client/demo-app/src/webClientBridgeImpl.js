const _ = require('lodash')
const axios = require('axios')
const config = require('config')

// ibm_url = 'https://api.au-syd.speech-to-text.watson.cloud.ibm.com/instances/cb821394-19b1-46f0-bb7d-721db18c9ec8'
let access_token = null

const getAccessToken = async () => {
    const resp = await axios.get('http://localhost:3009/ibm/token')
    return _.get(resp, 'data.access_token', null)
}


let wsclient = null
const sttIBMWebsocket = {

  sttGetConfig: async () => {
    return {
      useMediaRecorder: true,
      interimResultTime: 50,
    }
  },


  sttStartListening: async (params) => {
    if (!access_token){
        access_token = await getAccessToken()
      }
    const [metadata] = params
    const sttConfig = await sttIBMWebsocket.sttGetConfig()
    const interim_results = _.get(sttConfig, 'interimResultTime', 0) > 0
    wsclient = new WebSocket(`wss://${config.ibm.url}?access_token=${access_token}`)
    wsclient.onopen = (event) => {
      wsclient.send(JSON.stringify({
        action: 'start',
        interim_results,
        'content-type': `audio/${metadata.audioMetadata.fileFormat}`,
      }))
    }

    wsclient.onmessage = (event) => {
      const data = JSON.parse(event.data)
      const results = _.get(data, 'results', [])
      if (results.length > 0) {
        const lastresult = _.get(results, `[${results.length - 1}]`)
        const m = {
          text: _.get(lastresult, 'alternatives[0].transcript', ''),
          final: _.get(lastresult, 'final'),
        }
        window.sap.cai.webclient.onSTTResult(m)
      }
    }

    wsclient.onclose = (event) => {
      console.log('OnClose')
    }

    wsclient.onerror = (event) => {
      console.log('OnError', JSON.stringify(event.data))
    }
  },

  sttStopListening: async () => {
    console.log('StopListening')
    const client = wsclient
    setTimeout(() => {
      if (client) {
        client.close()
      }
    }, 5000)
  },

  sttAbort: async () => {
    if (wsclient) {
      wsclient.close()
      wsclient = null
    }
  },

  sttOnInterimAudioData: async (params) => {
    if (wsclient) {
      const [blob, metadata] = params
      wsclient.send(blob)
    }
  },

  sttOnFinalAudioData: async (params) => {
    if (wsclient) {
      const [blob, metadata] = params
      console.log('Sending final blob')
      wsclient.send(blob)
      console.log('Sending stop')
      wsclient.send(JSON.stringify({
        action: 'stop',
      }))
    }
  },
}


export default sttIBMWebsocket