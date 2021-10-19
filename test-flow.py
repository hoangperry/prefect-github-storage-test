import prefect
from prefect import task, Flow
from prefect.run_configs import LocalRun, UniversalRun


from prefect import Flow
from prefect.storage import GitHub

flow = Flow(
	"hello-flow-2", 
	storage=GitHub(
	    repo="hoangperry/prefect-github-storage-test",
	    path="test-flow.py",
	    access_token_secret="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"
	), 
	run_config=UniversalRun(labels=["mail.kernel.vn"])
)

@task
def say_hello():
	logger = prefect.context.get("logger")
	logger.info("Hello, Cloud!")


say_hello.run()

# Register the flow under the "tutorial" project
flow.register(project_name="test")
