package example

import (
	"context"
	"fmt"
	"github.com/nakiner/lector"
	"os"
)

type Service interface {
	ListenElection(ctx context.Context)
}

type service struct {
	svc      lector.Service
	identity string
}

func NewService(namespace string, podName string) (Service, error) {
	lectorSvc, err := lector.NewService(namespace, podName)
	if err != nil {
		return nil, err
	}

	return &service{svc: lectorSvc}, nil
}

func (s *service) LeaderStartCallback(ctx context.Context) {
	fmt.Fprintln(os.Stdout, "i am leader, need to do work")
	s.PerformRunAction(ctx)
}

func (s *service) LeaderSelectCallback(str string) {
	fmt.Fprintln(os.Stdout, fmt.Sprintf("whoa %s chosen one!", str))
	if str == s.identity {
		fmt.Fprintln(os.Stdout, "wow it was me!")
	}
}

func (s *service) LeaderStopCallback() {
	fmt.Fprintln(os.Stdout, "i am not leader anymore")
}

func (s *service) ListenElection(ctx context.Context) {
	pod, err := s.svc.Discovery().GetCurrentInstance(ctx)
	if err != nil {
		fmt.Fprintln(os.Stdout, "cannot get active instance")
		return
	}
	s.identity = pod.Name
	ctxNow, cancelFunc := context.WithCancel(ctx)

	err = s.svc.Elector().StartElection(ctxNow, cancelFunc, s.identity, s.LeaderStartCallback, s.LeaderSelectCallback, s.LeaderStopCallback, "serviceName")
	if err != nil {
		fmt.Fprintln(os.Stdout, "cannot listen election")
		return
	}
}

func (s *service) PerformRunAction(ctx context.Context) {
	fmt.Fprintln(os.Stdout, "i am doing very hard work after start")
}
