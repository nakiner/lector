package lector

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	k8err "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ElectorInterface interface {
	GetMasterLabels() (string, string)
	GetSlaveLabels() (string, string)
	GetAppLabel() string
	SetMasterLabel(ctx context.Context, pod *v1.Pod) error
	SetSlaveLabel(ctx context.Context, pod *v1.Pod) error
	StartElection(ctx context.Context, cancelFunc context.CancelFunc, id string, start LeaderStarted,
		selected LeaderSelected, stop LeaderStopped, leaseName string) error
}

type LeaderStarted func(ctx context.Context)
type LeaderSelected func(identity string)
type LeaderStopped func()

type ElectorService struct {
	service *service
}

func (e *ElectorService) StartElection(ctx context.Context, cancelFunc context.CancelFunc, id string, start LeaderStarted,
	selected LeaderSelected, stop LeaderStopped, leaseName string) error {
	leaseLockName := fmt.Sprintf("%s-lock", leaseName)
	leaseLockNamespace := e.service.Namespace
	client := e.service.ClientSet
	defer cancelFunc()

	if id == "" {
		return errors.New("Empty identity")
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		fmt.Println("Received termination, signaling shutdown")
		cancelFunc()
	}()

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseLockName,
			Namespace: leaseLockNamespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	leCfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: start,
			OnStoppedLeading: stop,
			OnNewLeader:      selected,
		},
	}

	leaderelection.RunOrDie(ctx, leCfg)

	return nil
}

func (e *ElectorService) GetMasterLabels() (string, string) {
	return "role", "master"
}

func (e *ElectorService) GetSlaveLabels() (string, string) {
	return "role", "slave"
}

func (e *ElectorService) GetAppLabel() string {
	return "app"
}

func (e *ElectorService) SetMasterLabel(ctx context.Context, pod *v1.Pod) error {
	key, val := e.GetMasterLabels()
	pod.Labels[key] = val
	if _, err := e.service.ClientSet.CoreV1().Pods(e.service.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err == nil {
		return nil
	} else {
		if k8err.IsConflict(err) {
			for i := 0; i < 5; i++ {
				time.Sleep(time.Second)
				pod, err = e.service.Discovery().GetInstanceInfo(ctx, pod.Name)
				if err != nil {
					return errors.Wrap(err, "err get pod retry")
				}
				pod.Labels[key] = val
				if _, err = e.service.ClientSet.CoreV1().Pods(e.service.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
					if k8err.IsConflict(err) {
						continue
					} else {
						return errors.Wrap(err, "err set master retry")
					}
				} else {
					return nil
				}
			}
			return errors.Wrap(err, "err set master retry failed")
		} else {
			return errors.Wrap(err, "err set master label")
		}
	}
}

func (e *ElectorService) SetSlaveLabel(ctx context.Context, pod *v1.Pod) error {
	key, val := e.GetSlaveLabels()
	pod.Labels[key] = val
	if _, err := e.service.ClientSet.CoreV1().Pods(e.service.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err == nil {
		return nil
	} else {
		if k8err.IsConflict(err) {
			for i := 0; i < 5; i++ {
				time.Sleep(time.Second)
				pod, err = e.service.Discovery().GetInstanceInfo(ctx, pod.Name)
				if err != nil {
					return errors.Wrap(err, "err get pod retry")
				}
				pod.Labels[key] = val
				if _, err = e.service.ClientSet.CoreV1().Pods(e.service.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
					if k8err.IsConflict(err) {
						continue
					} else {
						return errors.Wrap(err, "err set slave retry")
					}
				} else {
					return nil
				}
			}
			return errors.Wrap(err, "err set slave retry failed")
		} else {
			return errors.Wrap(err, "err set slave label")
		}
	}
}
