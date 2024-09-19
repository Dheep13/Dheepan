import pandas as pd

def parse_parameter_file_for_workflows(filename):
    workflow = ""
    variables = []
    data = []
    capturing_workflow = False

    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            
            if line.startswith("[AIA.WF:") and ".ST:" not in line and ".WT:" not in line:
                # When encountering a new workflow with no session or worklet
                if capturing_workflow and workflow and variables:
                    data.append({
                        "Workflow Name": workflow,
                        "Variables": "\n".join(variables)
                    })
                workflow = line.split(':')[-1].split(']')[0]
                variables = []
                capturing_workflow = True
                print(f"New Workflow detected: {workflow}")

            elif line.startswith("[AIA.WF:") and (".ST:" in line or ".WT:" in line):
                # This is a session or worklet line, so stop capturing variables for the current workflow
                capturing_workflow = False

            elif capturing_workflow and line.startswith("$$"):
                # Collect variables associated with the workflow
                variables.append(line)

            elif line == "" and capturing_workflow and workflow and variables:
                # Handle empty lines and ensure data is added to the list
                data.append({
                    "Workflow Name": workflow,
                    "Variables": "\n".join(variables)
                })
                workflow = ""
                variables = []
                capturing_workflow = False

        # After finishing file, add last workflow if applicable
        if capturing_workflow and workflow and variables:
            data.append({
                "Workflow Name": workflow,
                "Variables": "\n".join(variables)
            })

    return pd.DataFrame(data)

# Example usage
filename = "AIA.PAR"
df = parse_parameter_file_for_workflows(filename)
df.to_excel("workflows_params_only.xlsx", index=False)
