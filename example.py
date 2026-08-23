import src.dynworkflow as d
app = d.Flow(
    f"action",
    agent=d.AgentClient("ws://127.0.0.1:7433/acp?k=a")
)

print("========")


@app.node("Scan Docs")
def scan_docs(fallback_info: int | None = None) -> d.TaskGenerator:
    print("Scan Docs Start")

    if fallback_info != None:  # 模拟数据
        result = "Success"
    else:
        result = yield d.Agent(
            "Summary the document, get the varible 'aaa' value",
            model="",
            path="docs/test"
        )
        print("Scan Docs Finished, result =", result)
        return check_step(val=0)

    print("Scan Docs Finished, result =", result)
    return check_step(val=1), scan_project(val2=2)


@app.node("Check")
def check_step(val: int) -> d.NodeResult:
    print("Run Check")
    if (val == 0):
        print("Check Failed!")
        return scan_docs(fallback_info=val)
    print("Check Success!")
    return scan_project(val=val)


@app.node("Scan Project")
def scan_project(val: int, val2: int) -> d.TaskGenerator:
    print("Scan Project Start")
    result = yield d.MultiAgent(
        d.Agent(f"Calc {val} + 1, DO NOT CALL ANY TOOLS!"),
        d.Agent(f"Calc {val2} + 1, DO NOT CALL ANY TOOLS!")
    )
    print("Scan Project Finished, result =", result)
    return d.Result(result)


app.run(scan_docs())
