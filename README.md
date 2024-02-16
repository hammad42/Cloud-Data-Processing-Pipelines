# Cloud Data Processing Pipeline

## Overview
The Cloud Data Processing Pipeline automates the processing of data files stored in a cloud storage bucket using Google Cloud Platform services. The pipeline is orchestrated by Google Cloud Composer and utilizes Google Cloud Dataproc, PySpark, Google BigQuery, and Google Cloud Storage.

## Features
- Automatically processes data files stored in a cloud storage bucket.
- Orchestrates a PySpark cluster on Google Cloud Dataproc.
- Executes PySpark jobs to extract and transform data.
- Loads processed data into Google BigQuery.
- Moves processed files to a designated zone within the cloud storage bucket.
- Cleans up the Dataproc cluster after processing.

## Components
- **Google Cloud Storage (GCS)**: Stores the input and output data files.
- **Google Cloud Composer**: Orchestrates the pipeline workflow.
- **Google Cloud Dataproc**: Manages the PySpark cluster for data processing.
- **PySpark**: Performs data extraction and transformation tasks.
- **Google BigQuery**: Stores the processed data.

## Usage
1. Clone the repository.
2. Set up Google Cloud Platform (GCP) project.
3. Enable necessary APIs: Google Cloud Composer, Google Cloud Dataproc, Google Cloud Storage, Google BigQuery.
4. Set up a service account with appropriate permissions for GCP services.
5. Configure the pipeline parameters and environment variables.
6. Upload data files to the designated processing zone in GCS.
7. Trigger the pipeline execution in Cloud Composer.
8. Monitor the pipeline progress and logs in Cloud Composer.
9. Verify the data loading and processing results in BigQuery.
10. Clean up resources after processing.

## Contributors
- [Hammad Shamim](https://www.linkedin.com/in/hammad-shamim-6a2344128/)

## License
This project is licensed under the [MIT License](LICENSE).

Feel free to customize this README.md according to your project's specific details and requirements. Good luck with your Cloud Data Processing Pipeline project!
