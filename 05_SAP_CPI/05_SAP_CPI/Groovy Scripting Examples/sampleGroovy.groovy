import com.sap.gateway.ip.core.customdev.util.Message

def Message processData(Message message) {
    def body = message.getBody(String)
    def headers = message.getHeaders()
    def properties = message.getProperties()

    message.setBody(DoMapping(body, headers, properties))

    return message
}

//Need comment this TestRun() before upload to CPI. This TestRun() for local debug only
//TestRun()

void TestRun(){
    def scriptDir = new File(getClass().protectionDomain.codeSource.location.toURI().path).parent
    def dataDir = scriptDir + "\\Data"

    Map headers = [:]
    Map props = [:]

    headers.put("field1", "John")
    props.put("text1", "How are you?")

    File inputFile = new File("$dataDir\\example.txt")
    File outputFile = new File("$dataDir\\example_output.txt")

    def inputBody = inputFile.getText("UTF-8")
    def outputBody = DoMapping(inputBody, headers, props)

    println "field1=" + headers.get("field1") as String
    println "text1=" +props.get("text1") as String
    println "field2=" + headers.get("field2") as String
    println "text2=" +props.get("text2") as String

    println outputBody
    outputFile.write outputBody
}

def DoMapping(String body, Map headers, Map properties) {
    String output = ""

    String v_field1 = headers.get("field1") as String
    String v_text1 = properties.get("text1") as String

    headers.put("field1", v_field1 + " (modified)")
    headers.put("field2", "This is field2 (new)")
    properties.put("text1", v_text1 + " (modified)")
    properties.put("text2", "This is text2 (new)")

    output = body + " (modified)"

    return output
}