# Bosh CPI for Kubernetes
This repository contains a Bosh CPI for Kubernetes. It shall enable people to deploy Bosh deployments on Kubernetes for Dev and Test.

## Important 
The Bosh CPI for Kubernetes is not there to provide a connector for production runtime on Kubernetes. 

Currently Bosh has some limitations in its design, which prevent a redeployment of failing Pods.

## Usage and Deployment

## Versions

Name | Version
------------ | -------------
Kubernetes | 1.8+

## Usage

To use this bosh release, first upload it to your bosh:

```
git clone git@https://github.com/evoila/kubernetes-cpi
cd kubernetes-cpi
```

To load the CI sub modules for an enablement of the building pipeline in course, please issue: 
```
git submodule init
git submodule update
``` 

Create release
```
bosh create-release --force
```
Upload release
```
bosh -e <ENVIRONMENT> upload-release
```

Then you have to modify your manifest. An example manifest you can find in manifest/*-dev-ENVIRONMENT.yml

Deploy release
```
bosh deploy -e <ENVIRONMENT> -d <DEPLOYMENTNAME> manifest/<YOURMANIFEST>.yml
```

## Contribution 

Welcome to contribute through pull request  

# Credits
This is a branch from repository which was original developed by: Matthew Sykes (https://github.com/sykesm)


# Installation

## Requirements
You have setup your Go environment with:
* Go Binaries 
* Go Path
* Godep installed `go get github.com/tools/godep`

## Getting started
After setting up the core environment the next step is to load the Kubernetes Go Client. This is a bit more of work, as we need to retrieve the specific version:

```shell
go get k8s.io/client-go/...
git checkout v5.0.0
cd client-go/
git checkout v5.0.0
godep restore ./...
```