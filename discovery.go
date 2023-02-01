package lector

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	k8err "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type DiscoveryInterface interface {
	GetMasterInstance(ctx context.Context, name string) (*v1.Pod, error)
	GetInstanceInfo(ctx context.Context, podName string) (*v1.Pod, error)
	GetInstances(ctx context.Context, name string) (*v1.PodList, error)
	GetCurrentInstance(ctx context.Context) (*v1.Pod, error)
	GetJobByName(ctx context.Context, jobName string) (*batchv1.Job, error)
	GetPodsByJob(ctx context.Context, name string) (*v1.PodList, error)
	GetJobs(ctx context.Context, name string) (finished *batchv1.JobList, failed *batchv1.JobList, pending *batchv1.JobList, err error)
}

type DiscoveryService struct {
	service *service
}

func (d *DiscoveryService) GetMasterInstance(ctx context.Context, name string) (*v1.Pod, error) {
	key, val := d.service.Elector().GetMasterLabels()
	selector := map[string]string{
		key:                               val,
		d.service.Elector().GetAppLabel(): name,
	}
	pods, err := d.service.ClientSet.CoreV1().Pods(d.service.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(selector).String(),
	})

	if k8err.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, errors.New("error fetching instances")
	}

	if len(pods.Items) < 1 {
		return nil, nil
	}

	if len(pods.Items) > 1 { // todo: unlock when election mechanism will work
		return nil, errors.New("error instance count mismatch")
	}

	return &pods.Items[0], nil
}

func (d *DiscoveryService) GetInstanceInfo(ctx context.Context, podName string) (*v1.Pod, error) {
	pod, err := d.service.ClientSet.CoreV1().Pods(d.service.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if k8err.IsNotFound(err) {
		return nil, errors.New("instance not found")
	} else if statusError, isStatus := err.(*k8err.StatusError); isStatus {
		return nil, errors.New(fmt.Sprintf("error getting instance %v\n", statusError.ErrStatus.Message))
	} else if err != nil {
		return nil, err
	}

	return pod, nil
}

func (d *DiscoveryService) GetInstances(ctx context.Context, name string) (*v1.PodList, error) {
	selector := map[string]string{
		d.service.Elector().GetAppLabel(): name,
	}
	pods, err := d.service.ClientSet.CoreV1().Pods(d.service.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(selector).String(),
	})
	if err != nil {
		return nil, errors.New("error fetching instances")
	}

	return pods, nil
}

func (d *DiscoveryService) GetCurrentInstance(ctx context.Context) (*v1.Pod, error) {
	if d.service.PodName == "" {
		return nil, nil
	}
	pod, err := d.service.ClientSet.CoreV1().Pods(d.service.Namespace).Get(ctx, d.service.PodName, metav1.GetOptions{})
	if k8err.IsNotFound(err) {
		return nil, errors.New("instance not found")
	} else if statusError, isStatus := err.(*k8err.StatusError); isStatus {
		return nil, errors.New(fmt.Sprintf("error getting instance %v\n", statusError.ErrStatus.Message))
	} else if err != nil {
		return nil, err
	}

	return pod, nil
}

func (d *DiscoveryService) GetJobs(ctx context.Context, name string) (*batchv1.JobList, *batchv1.JobList, *batchv1.JobList, error) {
	finished := batchv1.JobList{Items: []batchv1.Job{}}
	failed := batchv1.JobList{Items: []batchv1.Job{}}
	pending := batchv1.JobList{Items: []batchv1.Job{}}

	labelSelector := map[string]string{
		d.service.Elector().GetAppLabel(): name,
	}

	jobs, err := d.service.ClientSet.BatchV1().Jobs(d.service.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labelSelector).String(),
	})
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "err get jobs")
	}

	for _, job := range jobs.Items {
		if job.Status.Succeeded > 0 {
			finished.Items = append(finished.Items, job)
		} else if job.Status.Failed > 0 {
			failed.Items = append(failed.Items, job)
		} else {
			pending.Items = append(pending.Items, job)
		}
	}

	return &finished, &failed, &pending, nil
}

func (d *DiscoveryService) GetJobByName(ctx context.Context, jobName string) (*batchv1.Job, error) {
	job, err := d.service.ClientSet.BatchV1().Jobs(d.service.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "err get jobByName")
	}

	return job, nil
}

func (d *DiscoveryService) GetPodsByJob(ctx context.Context, name string) (*v1.PodList, error) {
	selector := map[string]string{
		"job-name": name,
	}
	pods, err := d.service.ClientSet.CoreV1().Pods(d.service.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(selector).String(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "err get pods by job")
	}
	return pods, nil
}
