# Python Data Engineering  

This repo contains some basic (Databricks) Jupiter notebooks which some basic testcases or examples. Also some basic Python setup to run Python scripts using Argo on a Kubernetes cluster.  

## Databricks  

Databricks example notebooks.  

## Jupyterhub  

Jupyterhub example notebooks.  

### Docker container for local usage (or helm/kubernetes)

Local docker container with jupyter notebook:

- [Jupyter Docker Stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html)
  
```bash
docker pull quay.io/jupyter/all-spark-notebook
```

## PythonScripts

Contains Python scripts to be used for ETL.

### Argo

Contains workflows to be used by Argo to run said Python scripts.

Login to the GUI with a Bearer token:

```bash
kubectl -n argo get secret argo-workflows-server-token -o jsonpath='{.data.token}' | base64 --decode
```

- [Argo Workflows Releases](https://github.com/argoproj/argo-workflows/releases/)

### Kubectl config

Make sure the VScode terminal will work with the remote Kubernetes cluster. This could be scripted if done a lot..  

Run local(inside .venv/bin/activate):

```bash
mkdir -p ~/.kube
scp user@remote_host:~/.kube/microk8s-config ~/.kube/microk8s-config
chmod 600 ~/.kube/microk8s-config
export KUBECONFIG=~/.kube/microk8s-config
```  

test it with:

```bash
kubectl config current-context
```

### Add scheduled workflow on Kubernetes with Argo

One:  

```bash
kubectl apply -f Argo/CronWorkflow/workflow_hello.yml
```

Or all at once:

```bash
kubectl apply -f Argo/CronWorkflow/
```

### Add secrets in Kubernetes cluster to be used by Python scripts

Secrets should be put in Kubernetes secret to be used by the Docker container. Docker container itself and the repo should not contain secrets.  

Possibility is to set it up with ansible-vault en create the secrets during a normal config run. 

```bash
kubectl create secret generic python-data-engineering \
    --namespace argo \
    --from-literal=NAME=xxx \
    --from-literal=AWS_ACCESS_KEY_ID=xxx \
    --from-literal=AWS_SECRET_ACCESS_KEY=xxx \
    --from-literal=S3_ENDPOINT_URL=xxx \
    --from-literal=S3_BUCKET=xxx \
    --dry-run=client -o yaml | kubectl apply -f -
```

### Set local env

Create a .env to contain the local envs to test locally using:

```python
from dotenv import load_dotenv
load_dotenv()
```

### Docker

Contains Dockerfile to create image containing the python scripts and needed dependencies.  

```bash
docker build -t python -f Docker/PythonImage.Dockerfile .
docker build -t polars -f Docker/PolarsImage.Dockerfile .
docker build -t dask -f Docker/DaskImage.Dockerfile .
```
