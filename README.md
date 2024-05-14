# Forex Data Pipeline Using Apache Airflow

## Introduction
This project implements a data pipeline using Apache Airflow for processing forex data.

## Installation and Setup
1. Clone the repository.
2. Install Airflow and its dependencies.
3. Configure Airflow connections and variables as needed.
4. Start the Airflow scheduler and webserver.

## DAG Overview
The Airflow DAG automates the process of downloading forex rates, processing the data, and storing it in HDFS.

## Usage
1. Ensure Airflow is up and running.
2. Trigger the `forex_data_pipeline2` DAG manually or schedule it to run daily.
3. Monitor the DAG execution in the Airflow UI.

## DAG Structure

![Flow of DAG](assets/airflow-dag.png)

### Working Video
[Watch the working video](link/to/your/working/video)

### Slack Notification
![Slack Notification](assets/project-working.png)

## Contributing
Contributions are welcome! Please read the [contribution guidelines](CONTRIBUTING.md) before submitting a pull request.

## License
This project is licensed under the [MIT License](LICENSE).
