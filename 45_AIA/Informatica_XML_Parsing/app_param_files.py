import pandas as pd

def parse_parameter_file(filename):
    workflow = ""
    worklet = ""
    session = ""
    variables = []
    data = []

    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            
            if line.startswith("[AIA.WF:"):
                # When encountering a new workflow, reset the workflow, worklet, and session names
                if workflow and (session or worklet):
                    print(f"Captured - Workflow: {workflow}, Worklet: {worklet}, Session: {session}, Variables: {variables}")
                    data.append({
                        "Workflow Name": workflow,
                        "Worklet Name": worklet,
                        "Session Name": session,
                        "Variables": "\n".join(variables)
                    })
                variables = []
                worklet = ""
                session = ""
                workflow = line.split(':')[-1].split(']')[0]
                print(f"New Workflow detected: {workflow}")

            elif ".WT:" in line and ".ST:" in line:
                # Line with both worklet and session
                parts = line.split(".WT:")
                workflow = parts[0].split("[AIA.WF:")[-1].strip()
                worklet, session = parts[1].split(".ST:")
                worklet = worklet.strip().split(']')[0]
                session = session.strip().split(']')[0]
                print(f"Worklet and Session detected - Workflow: {workflow}, Worklet: {worklet}, Session: {session}")

            elif ".ST:" in line:
                # Line with session only, no worklet
                if ".WT:" in line:
                    parts = line.split(".WT:")
                    workflow = parts[0].split("[AIA.WF:")[-1].strip()
                    worklet, session = parts[1].split(".ST:")
                    worklet = worklet.strip().split(']')[0]
                    session = session.strip().split(']')[0]
                else:
                    session = line.split(".ST:")[-1].split(']')[0]
                    worklet = ""  # No worklet
                print(f"Session detected - Workflow: {workflow}, Session: {session}")

            elif line.startswith("$$"):
                # Collect variables associated with the workflow/worklet/session
                variables.append(line)

            if line == "" and workflow and (session or worklet):
                # Handle empty lines and ensure data is added to the list
                print(f"Adding Data - Workflow: {workflow}, Worklet: {worklet}, Session: {session}")
                data.append({
                    "Workflow Name": workflow,
                    "Worklet Name": worklet,
                    "Session Name": session,
                    "Variables": "\n".join(variables)
                })
                variables = []
                worklet = ""
                session = ""

        # After finishing file, add last workflow if applicable
        if workflow and (session or worklet):
            print(f"Final Addition - Workflow: {workflow}, Worklet: {worklet}, Session: {session}")
            data.append({
                "Workflow Name": workflow,
                "Worklet Name": worklet,
                "Session Name": session,
                "Variables": "\n".join(variables)
            })

    return pd.DataFrame(data)

# Example usage
filename = "AIA.PAR"
df = parse_parameter_file(filename)
df.to_excel("parsed_parameters.xlsx", index=False)
