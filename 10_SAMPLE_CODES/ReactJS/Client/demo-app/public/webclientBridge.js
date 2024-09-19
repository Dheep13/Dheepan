const webclientBridge = {

    callImplMethod: async (name, ...args) => {
    if (window.webclientBridgeImpl && window.webclientBridgeImpl[name]) {
         return window.webclientBridgeImpl[name](...args)
    }
 },

    sttGetConfig: async (...args) => {
    return webclientBridge.callImpMethod('sttGetConfig', ...args)
    },
    sttStartListening: async (...args) => {
    return webclientBridge.callImplMethod('sttStartListening', ...args) },
    
    sttStopListening: async (...args) => {
    return webclientBridge.callImplMethod('sttStoplistening', ...args)
    },
    
    sttAbort: async (...args) => {
    return webclientBridge.callImpMethod('sttAbort', ...args)
    },

    sttonFinalAudioData: async (...args) => {
    return webclientBridge.callImpMethod('sttonFinalAudioData', ...args)
    },
    
    sttonInterimAudioData: async (...args) => {
    return webclientBridge.callImpMethod('sttonInterimAudioData', ...args)
    }
}
    window.sapcai = { webclientBridge,
    }