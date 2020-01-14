# Run Insights on Flights Data  with Kafka, Airflow and Spark using Docker Compose
### The MicroService Based Architecture linked together 

This is Like Story Telling , I want to know some insights from flight transactions

Let's say we got a ML model that has been put in production and is actively serving predictions. Simultaneously, we got new training data that becomes available in a streaming way while users use the model. Incrementally updating the model with new data can improve the model, whilst it also might reduce model drift. However, it often comes with additional overhead. Luckily, there are tools that allow you to automate many parts of this process. 

This repository takes on the topic of incrementally updating a ML model as new data becomes available. It mainly leans on three nifty tools, being [Kafka](https://github.com/apache/kafka), [Airflow](https://github.com/apache/airflow), and [MLFlow](https://github.com/mlflow/mlflow). 

The corresponding [walkthrough/post](https://medium.com/vantageai/keeping-your-ml-model-in-shape-with-kafka-airflow-and-mlflow-143d20024ba6) on Medium lays out the workings of this repo step-by-step.
