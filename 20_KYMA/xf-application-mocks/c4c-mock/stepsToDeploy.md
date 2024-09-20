# c4c-mock Kyma Deployment Guide

This guide provides step-by-step instructions for deploying the c4c-mock application to a Kyma cluster.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Deployment Steps](#deployment-steps)
3. [Troubleshooting](#troubleshooting)
4. [Accessing the Application](#accessing-the-application)
5. [Summary](#summary)

## Prerequisites

- Access to a Kyma cluster
- kubectl installed and configured to connect to your Kyma cluster
- The c4c-mock application files

## Deployment Steps

### 1. Verify Cluster Connection

Ensure you're connected to your Kyma cluster:

```bash
kubectl cluster-info
```

### 2. Create and Set Namespace

Create a new namespace and set it as the current context:

```bash
kubectl create namespace mocks
kubectl config set-context --current --namespace=mocks
```

### 3. Deploy Kubernetes Resources

Apply the k8s.yaml file to create necessary resources:

```bash
kubectl apply -f https://raw.githubusercontent.com/SAP/xf-application-mocks/main/c4c-mock/deployment/k8s.yaml
```

### 4. Check APIRule Version

Verify the available APIRule version:

```bash
kubectl api-resources | Select-String apirul
```

### 5. Update APIRule Definition

Create or update the `kyma.yaml` file with the following content:

```yaml
apiVersion: gateway.kyma-project.io/v1beta1
kind: APIRule
metadata:
  name: c4c-mock
spec:
  gateway: kyma-gateway.kyma-system.svc.cluster.local
  host: c4c
  service:
    name: c4c-mock
    port: 10000
  rules:
    - path: /.*
      methods: ["GET", "POST", "PUT", "DELETE", "HEAD"]
      accessStrategies:
        - handler: noop
```

### 6. Apply APIRule

Apply the updated APIRule:

```bash
kubectl apply -f deployment/kyma.yaml
```

### 7. Verify APIRule Creation

Check if the APIRule was created successfully:

```bash
kubectl get apirules c4c-mock
```

### 8. Get Application URL

Retrieve the host part of the URL:

```bash
kubectl get apirule c4c-mock -o jsonpath='{.spec.host}'
```

The full URL will be `https://<host>.<cluster-domain>`

## Troubleshooting

If you encounter issues during deployment, try the following:

- Ensure you're in the correct namespace:
  ```bash
  kubectl config view --minify --output 'jsonpath={..namespace}'
  ```

- Verify that the c4c-mock service exists:
  ```bash
  kubectl get services c4c-mock
  ```

- Check the status and details of the APIRule:
  ```bash
  kubectl describe apirule c4c-mock
  ```

- If the APIRule isn't recognized, check the API version:
  ```bash
  kubectl api-versions | Select-String gateway.kyma-project.io
  ```

## Accessing the Application

After successful deployment, the c4c-mock application is accessible at:

```
https://c4c.c-087b199.kyma.ondemand.com/
```

Replace the domain with your actual Kyma cluster domain.

## Summary

This deployment process involves the following key steps:
1. Connecting to the Kyma cluster
2. Creating and setting the correct namespace
3. Deploying the application resources
4. Creating an APIRule to expose the application
5. Verifying the deployment and obtaining the URL

Remember to always check the current API version of resources you're deploying and ensure you're in the correct namespace before applying resources.

For future deployments, you may need to adjust the APIRule and other resources based on the specific requirements of the application you're deploying. The general process, however, will remain similar.