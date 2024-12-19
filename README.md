# Python Data Engineering  

This repo contains some basic (Databricks) Jupiter notebooks which some basic testcases or examples. Also some basic Python setup to run Python scripts using Argo on a Kubernetes cluster.  

## Databricks  

Databricks example notebooks.  

## Jupyterhub  

Jupyterhub example notebooks.  

### Docker container for local usage (or helm/kubernetes)

Local docker container with jupyter notebook:

- [Jupyter Docker Stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html)
- ```docker pull quay.io/jupyter/all-spark-notebook```

## PythonScripts

Contains Python scripts to be used for ETL.

### Argo

Contains workflows to be used by Argo to run said Python scripts. 

- [Argo Workflows Releases](https://github.com/argoproj/argo-workflows/releases/)

### Kubectl config

Make sure the VScode terminal will work with the remote Kubernetes cluster. This could be scripted if done a lot..  

Run remote:
- generate config with ```microk8s config > ~/.kube/microk8s-config``` 
- check content with ```more ~/.kube/microk8s-config```

Run local:
- ```scp user@remote_host:~/.kube/microk8s-config ~/.kube/microk8s-config```
- ```export KUBECONFIG=~/.kube/microk8s-config``` inside .venv/bin/activate
- test it with ```kubectl config current-context```

### Add scheduled workflow on Kubernetes with Argo

One:  

- ```kubectl apply -f Argo/CronWorkflow/workflow_hello.yml```

Or all at once:

- ```kubectl apply -f Argo/CronWorkflow/```

### Add secrets in Kubernetes cluster to be used by Python scripts

Secrets should be put in Kubernetes secret to be used by the Docker container. Docker container itself and the repo should not contain secrets.  

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

### To do

- Schedule all workflows on argo as a step in the github workflow.  
- Set/update the kubernetes secret (safely) using workflow.  
