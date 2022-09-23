# usage
Package provides different methods, such as electing master node between instances, instance discovery for 
searching instances and jobs in cluster, and also controller services with abilities to start independent tasks in cluster via batchv1/job api. 

# installation
`go get -u github.com/nakiner/lector`

# example

	svc, err := example.NewService()
	if err != nil {
		level.Error(logger).Log("err", err, "msg", "kube function unavailable")
	} else {
        go svc.ListenElection(ctx)
    }

Customize callbacks inside example on your needs

# environment

to use in production packages required some variables, such as:
    
    SERVICE_NAME_SD_K8S_ENABLED: true
    SERVICE_NAME_SD_K8S_NAMESPACE: metadata.namespace
    SERVICE_NAME_SD_K8S_POD_NAME: metadata.name

Helm charts should contain ServiceAccount and roles should be updated according to privileges, used in package, ex:

    {{- if .Values.rbac.enabled }}
    apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      name: {{ template "builder.fullname" $ }}
      labels:
    {{ include "builder.labels.standard" . | indent 4 }}
    rules:
      - apiGroups: ["","batch","coordination.k8s.io"]
        resources: ["pods","jobs","leases"]
        verbs: ["get","list","watch"]
    {{- end }}