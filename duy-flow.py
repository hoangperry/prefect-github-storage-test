import prefect
from prefect import task, Flow
from prefect.run_configs import LocalRun, UniversalRun
from prefect.engine.results import LocalResult

import sqlalchemy
import pandas as pd
from prefect import task, Flow 
from prefect.storage import GitHub


@task 
def preprocess_data():
    engine = sqlalchemy.create_engine("postgresql://hoang:hoangtest111@ez4cast.vn:5433/flowdb") 
    query = """
    SET datestyle = dmy;
    SELECT
        DATE("InvoiceDate") AS date,
        SUM(to_number("TotalPayment", regexp_replace(replace("TotalPayment", ',', 'G'), '[0-9]', '9', 'g'))) AS revenue
    FROM
        sale_data_1
    GROUP BY
        "InvoiceDate"
    ORDER BY
        date
    """

    df = pd.read_sql_query(query, con=engine)
    print("Loading data succesfully...")
    print(df.head())
    return df


@task 
def predict_pmdarima(df):
    print("Training...")
    x = df["revenue"]
    from sktime.forecasting.arima import AutoARIMA
    forecaster = AutoARIMA(start_p=8, max_p=9, suppress_warnings=True) 
    forecaster.fit(x)
    print(forecaster.summary())

 
@task
def predict_statsmodel(df):
    from statsmodels.tsa.arima.model import ARIMA

    print("Training...")
    x = df["revenue"].tolist() 
    model = ARIMA(x, order=(1, 1, 1))
    model = model.fit()
    y_hat = model.predict(len(x), len(x), typ='levels')[0]
    print(f"Predict value: {y_hat}")


with Flow(
		"the_first_revenue_prediction", 
		result=LocalResult(),
		storage=GitHub(
			repo="hoangperry/prefect-github-storage-test",
			path="duy-flow.py",
		), 
		run_config=UniversalRun(labels=["mail.kernel.vn"]),
	) as flow:
    df = preprocess_data()
    predict_pmdarima(df) 
    predict_statsmodel(df)


# Register the flow under the "tutorial" project
flow.register(project_name="test")
