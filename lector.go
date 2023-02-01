package lector

import (
	"github.com/pkg/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Service interface {
	Discovery() DiscoveryInterface
	Elector() ElectorInterface
	Controller() ControllerInterface
}

type service struct {
	Namespace string
	PodName   string
	ConfigSet *rest.Config
	ClientSet *kubernetes.Clientset
}

func NewService(namespace string, podName string) (Service, error) {
	svc := service{
		Namespace: namespace,
		PodName:   podName,
	}

	if err := svc.FetchCluster(); err != nil {
		return nil, errors.Wrap(err, "error create")
	}

	return &svc, nil
}

func (s *service) FetchCluster() error {
	if s.Namespace == "" {
		return errors.New("namespace empty")
	}

	if s.PodName == "" {
		return errors.New("current pod empty")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "error not in cluster")
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "error fetch client")
	}

	s.ConfigSet = config
	s.ClientSet = client

	return nil
}

func (s *service) Discovery() DiscoveryInterface {
	return &DiscoveryService{service: s}
}

func (s *service) Elector() ElectorInterface {
	return &ElectorService{service: s}
}

func (s *service) Controller() ControllerInterface {
	return &ControllerService{service: s}
}
