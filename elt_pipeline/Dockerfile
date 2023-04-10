FROM python:3.9.16-slim

# Install spark and java
ARG openjdk_version="17"

RUN apt-get update --yes && \
    apt-get install --yes "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Add repository code
WORKDIR /opt/dagster/app/elt_pipeline  
COPY . /opt/dagster/app/elt_pipeline
COPY requirements.txt /opt/dagster/app/elt_pipeline
RUN pip install --upgrade pip && pip install -r requirements.txt

# CMD allows this to be overridden from run launchers or executors that want to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "elt_pipeline"]
