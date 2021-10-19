import prefect
from prefect import task, Flow
from prefect.run_configs import LocalRun, UniversalRun


from prefect import Flow
from prefect.storage import GitHub

flow = Flow(
    "github-flow",
    GitHub(
        repo="org/repo",                           # name of repo
        path="flows/my_flow.py",                   # location of flow file in repo
        access_token_secret="GITHUB_ACCESS_TOKEN"  # name of personal access token secret
    )
)

@task
def say_hello():
	logger = prefect.context.get("logger")
	logger.info("Hello, Cloud!")

with Flow("hello-flow-2", GitHub(
        repo="hoangperry/prefect-github-storage-test",                           # name of repo
        path="test-flow.py",                   # location of flow file in repo
        access_token_secret="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"  # name of personal access token secret
    ), run_config=UniversalRun(labels=["mail.kernel.vn"])) as flow:
	say_hello()

# Register the flow under the "tutorial" project
flow.register(project_name="test")
