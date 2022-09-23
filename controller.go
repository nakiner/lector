package lector

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lbs "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
)

type ControllerInterface interface {
	CreateNewJob(ctx context.Context, podParentName string, jobName string, jobImage string, args map[string]string, labels map[string]string) (*batchv1.Job, error)
	ClearJobHistory(ctx context.Context, name string, after time.Duration) error
	SuspendJob(ctx context.Context, name string) error
	WatchJob(ctx context.Context, name string, selector map[string]string) (watch.Interface, error)
}

type ControllerService struct {
	service *service
}

func (o *ControllerService) CreateNewJob(ctx context.Context, podParentName string, jobName string, jobImage string,
	args map[string]string, labels map[string]string) (*batchv1.Job, error) {
	if podParentName == "" {
		return nil, errors.New("Empty pod parent name")
	}

	if jobName == "" {
		return nil, errors.New("Empty job name")
	}

	if jobImage == "" {
		return nil, errors.New("Empty image")
	}

	pod, err := o.service.Discovery().GetInstanceInfo(ctx, podParentName)
	if err != nil {
		return nil, errors.Wrap(err, "err create job")
	}

	var argsf []string
	for key, value := range args {
		argsf = append(argsf, fmt.Sprintf("--%s=%s", key, value))
	}

	var envs []apiv1.EnvVar
	for _, row := range pod.Spec.Containers {
		envs = append(envs, row.Env...)
	}

	labels["parent"] = pod.Name
	labels[o.service.Elector().GetAppLabel()] = jobName

	respawnLimit := int32(0)

	jb := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s", pod.Name, jobName),
			Namespace:    o.service.Namespace,
			Labels:       labels,
		},
		Spec: batchv1.JobSpec{
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: fmt.Sprintf("%s-%s", pod.Name, jobName),
					Labels: map[string]string{
						o.service.Elector().GetAppLabel(): jobName,
					},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:  fmt.Sprintf("%s-jb", jobName),
							Image: jobImage,
							Args:  argsf,
							Env:   envs,
						},
					},
					RestartPolicy: apiv1.RestartPolicyNever,
				},
			},
			BackoffLimit: &respawnLimit,
		},
	}

	job, err := o.service.ClientSet.BatchV1().Jobs(o.service.Namespace).Create(ctx, jb, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "err create job")
	}

	return job, nil
}

func (o *ControllerService) ClearJobHistory(ctx context.Context, name string, after time.Duration) error {
	finished, failed, _, err := o.service.Discovery().GetJobs(ctx, name)
	if err != nil {
		return errors.Wrap(err, "err clearHistory get completed jobs")
	}

	for _, job := range finished.Items {
		t := job.Status.CompletionTime.Time.Add(after)
		if time.Now().After(t) {
			if err = o.SuspendJob(ctx, job.Name); err != nil {
				return errors.Wrap(err, fmt.Sprintf("err clearHistory delete job %s", job.Name))
			}
		}
	}

	for _, job := range failed.Items {
		for _, cond := range job.Status.Conditions {
			t := cond.LastTransitionTime.Time.Add(after)
			if time.Now().After(t) {
				if err = o.SuspendJob(ctx, job.Name); err != nil {
					return errors.Wrap(err, fmt.Sprintf("err clearHistory delete job %s", job.Name))
				}
				break
			}
		}
	}

	return nil
}

func (o *ControllerService) SuspendJob(ctx context.Context, name string) error {
	fg := metav1.DeletePropagationBackground
	if err := o.service.ClientSet.BatchV1().Jobs(o.service.Namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &fg,
	}); err != nil {
		return errors.Wrap(err, "err suspendJob remove job")
	}
	return nil
}

func (o *ControllerService) WatchJob(ctx context.Context, name string, selector map[string]string) (watch.Interface, error) {
	selector[o.service.Elector().GetAppLabel()] = name

	watcher, err := o.service.ClientSet.BatchV1().Jobs(o.service.Namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: lbs.SelectorFromSet(selector).String(),
	})

	if err != nil {
		return nil, errors.New("could not start watcher")
	}

	return watcher, nil
}
