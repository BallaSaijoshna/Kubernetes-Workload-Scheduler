package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Scheduler struct {
	client kubernetes.Interface
}

type Workload struct {
	Name   string `json:"name"`
	Image  string `json:"image"`
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

func NewScheduler(client kubernetes.Interface) *Scheduler {
	return &Scheduler{
		client: client,
	}
}

// Find a Ready and schedulable Kubernetes node
func (s *Scheduler) findNode() (*corev1.Node, error) {

	nodes, err := s.client.
		CoreV1().
		Nodes().
		List(
			context.Background(),
			metav1.ListOptions{},
		)

	if err != nil {
		return nil, err
	}

	for _, node := range nodes.Items {

		// Skip nodes marked unschedulable
		if node.Spec.Unschedulable {
			continue
		}

		// Check whether node is Ready
		for _, condition := range node.Status.Conditions {

			if condition.Type == corev1.NodeReady &&
				condition.Status == corev1.ConditionTrue {

				return &node, nil
			}
		}
	}

	return nil, fmt.Errorf("no ready Kubernetes node available")
}

// Create workload on selected node
func (s *Scheduler) scheduleWorkload(
	workload Workload,
) error {

	node, err := s.findNode()

	if err != nil {
		return err
	}

	log.Println(
		"Selected Node:",
		node.Name,
	)

	cpu := resource.MustParse(workload.CPU)
	memory := resource.MustParse(workload.Memory)

	pod := &corev1.Pod{

		ObjectMeta: metav1.ObjectMeta{

			Name: workload.Name,

			Namespace: "default",

			Labels: map[string]string{
				"app": workload.Name,
			},
		},

		Spec: corev1.PodSpec{

			// Assign workload to selected node
			NodeName: node.Name,

			Containers: []corev1.Container{

				{
					Name:  workload.Name,
					Image: workload.Image,

					Resources: corev1.ResourceRequirements{

						Requests: corev1.ResourceList{

							corev1.ResourceCPU: cpu,

							corev1.ResourceMemory: memory,
						},

						Limits: corev1.ResourceList{

							corev1.ResourceCPU: cpu,

							corev1.ResourceMemory: memory,
						},
					},

					Ports: []corev1.ContainerPort{

						{
							ContainerPort: 80,
						},
					},
				},
			},
		},
	}

	_, err = s.client.
		CoreV1().
		Pods("default").
		Create(
			context.Background(),
			pod,
			metav1.CreateOptions{},
		)

	if err != nil {
		return err
	}

	log.Println(
		"Workload scheduled successfully:",
		workload.Name,
	)

	return nil
}

// Monitor workload health
func (s *Scheduler) monitorWorkloads() {

	ticker := time.NewTicker(
		10 * time.Second,
	)

	defer ticker.Stop()

	for range ticker.C {

		pods, err := s.client.
			CoreV1().
			Pods("default").
			List(
				context.Background(),
				metav1.ListOptions{},
			)

		if err != nil {
			log.Println(
				"Health check error:",
				err,
			)

			continue
		}

		for _, pod := range pods.Items {

			log.Printf(
				"Workload: %s | Status: %s | Node: %s",
				pod.Name,
				pod.Status.Phase,
				pod.Spec.NodeName,
			)
		}
	}
}

// REST API
func (s *Scheduler) scheduleHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	if r.Method != http.MethodPost {

		http.Error(
			w,
			"Only POST method is allowed",
			http.StatusMethodNotAllowed,
		)

		return
	}

	workload := Workload{

		Name:   "demo-workload",
		Image:  "nginx:latest",
		CPU:    "100m",
		Memory: "128Mi",
	}

	err := s.scheduleWorkload(
		workload,
	)

	if err != nil {

		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)

		return
	}

	w.WriteHeader(
		http.StatusCreated,
	)

	fmt.Fprintln(
		w,
		"Workload scheduled successfully",
	)
}

// Health API
func healthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	w.WriteHeader(
		http.StatusOK,
	)

	fmt.Fprintln(
		w,
		"Scheduler is healthy",
	)
}

func main() {

	// Connect to Kubernetes from inside the cluster
	config, err := rest.InClusterConfig()

	if err != nil {

		log.Fatal(
			"Kubernetes connection failed:",
			err,
		)
	}

	clientset, err := kubernetes.NewForConfig(
		config,
	)

	if err != nil {

		log.Fatal(err)
	}

	scheduler := NewScheduler(
		clientset,
	)

	// Start workload monitoring
	go scheduler.monitorWorkloads()

	// REST APIs
	http.HandleFunc(
		"/health",
		healthHandler,
	)

	http.HandleFunc(
		"/schedule",
		scheduler.scheduleHandler,
	)

	log.Println(
		"Kubernetes Workload Scheduler started",
	)

	log.Println(
		"Server running on port 8080",
	)

	err = http.ListenAndServe(
		":8080",
		nil,
	)

	if err != nil {
		log.Fatal(err)
	}
}
