# This file install a custom Docker image for the external bronze pipeline

# Base Airflow image
FROM apache/airflow:3.1.5

# Install custom dependencies for the image
# User tells docker to run the next commands as user named airflow. Airflow images are desgined to run Airflow as airflow user for security and file-permissions consistency, if root was used the install would be OS level.
USER airflow 
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

