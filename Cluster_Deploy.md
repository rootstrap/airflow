# Steps to install airflow in Kubernetes 

## Create kubernates cluster with terraform 

1. Clone repository     
	```bash
	   git clone https://github.com/rootstrap/eks-base 
	```
2. Export variables 
	```bash
		export AWS_PROFILE=default
		export AWS_DEFAULT_REGION=us-east-1
	```
3. Copy file variables.tf to terraform variables terraform/variables.tf 
4. Create cluster    
	```bash
	    cd eks-base/terraform
		terraform init
		terraform apply
		Fill provider.aws.region with us-east-1
	```
4. Authenticate to the cluster 
   ```bash
   		aws eks update-kubeconfig --name mlcluster-tf-eks-cluster
	```
5. Register nodes to cluster
	 ```bash 
		terraform output config_map_aws_auth > config_map_aws_auth.yml
		kubectl apply -f config_map_aws_auth.yml
	 ```
6. Create ingress
	```bash 
		kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-0.32.0/deploy/static/provider/aws/deploy.yaml
	```

## Install airflow in kubernetes cluster with helm 

1. Move to chart directory 
   cd chart
2. Create namespace
   ```bash 
   		kubectl create namespace airflow
   	```
3. Set kubernetes namespace as default: 
	```bash 
		kubectl config set-context --current --namespace=airflow
	```
4. Create ssh keys and add them as deploy keys in the github repository, replace the email with the corresponding value
   ```bash 
   		ssh-keygen -t rsa -b 4096 -C "example@example.com"
   	```
5. Add repo 
   ```bash 
   		helm repo add stable https://charts.helm.sh/stable/
   	```

6. Create kubernetes secret for ssh keys, replacing {KEY_NAME} with the corresponding value according what has been generated in the last step.   
   ```bash
	   CURRENT_DIR=$(pwd)
	   kubectl create secret generic ssh-key-secret --from-file=ssh-privatekey=$CURRENT_DIR/{KEY_NAME}.id_rsa --from-file=ssh-publickey=$CURRENT_DIR/{KEY_NAME}.id_rsa.pub
	```
5. Install cluster
   ```bash
   		helm install airflow . --namespace airflow
   	```

6. Update 
   ```bash
	   If you update the charts and need to update the cluster, execute: 
	   helm upgrade airflow . --namespace airflow
	```

** Check database creation** 
```bash
	POSTGRES_POD=$(kubectl get pods | grep postgres | awk '{print $1}')
	kubectl exec -ti $POSTGRES_POD -- bash 
```
Login into database:   
```bash
	psql airflow airflow
```

List databases: 
```bash
	\l
```

List tables: 
```bash
	\dt 
```

Query users table:
```bash
select * from ab_user;
```

**Forwarding to access the web:**    
```bash
	kubectl port-forward $(kubectl get pods | awk '{print $1}' | grep 'webserver') 8080:8080
```

**Adding user to the cluster**   
```bash
	kubectl edit -n kube-system configmap/aws-auth
```
Add the following lines for the specific user: 

The new user can be logged in executing the following command: 
```bash
	aws eks update-kubeconfig --name mlcluster-tf-eks-cluster
```

```bash
- userarn: arn:aws:iam::XXXXYYYYZZZZ:user/ops-user
      username: ops-user
      groups:
        - system:masters 
```



