package reservations

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta2"
	sparkschedulerlisters "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/listers/sparkscheduler/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type Store interface {
	List(ctx context.Context) ([]*v1beta2.ResourceReservation, error)
	Get(ctx context.Context, namespace, name string) (*v1beta2.ResourceReservation, error)
	Create(ctx context.Context, rr *v1beta2.ResourceReservation) error
	Update(ctx context.Context, rr *v1beta2.ResourceReservation) error
}

type defaultStore struct {
	sparkschedulerV1beta2Interface sparkschedulerclient.ResourceReservationsGetter
	resourceReservationLister      sparkschedulerlisters.ResourceReservationLister
}

func NewDefaultStore(
	sparkschedulerV1beta2Interface sparkschedulerclient.ResourceReservationsGetter,
	resourceReservationLister sparkschedulerlisters.ResourceReservationLister,
) Store {
	return &defaultStore{
		sparkschedulerV1beta2Interface: sparkschedulerV1beta2Interface,
		resourceReservationLister:      resourceReservationLister,
	}
}

func (d *defaultStore) Get(ctx context.Context, namespace, name string) (*v1beta2.ResourceReservation, error) {
	return d.resourceReservationLister.ResourceReservations(namespace).Get(name)
}

func (d *defaultStore) List(ctx context.Context) ([]*v1beta2.ResourceReservation, error) {
	return d.resourceReservationLister.List(labels.Everything())
}

func (d *defaultStore) Create(ctx context.Context, obj *v1beta2.ResourceReservation) error {
	_, err := d.sparkschedulerV1beta2Interface.ResourceReservations(obj.GetNamespace()).Create(ctx, obj, metav1.CreateOptions{})
	return err
}

func (d *defaultStore) Update(ctx context.Context, obj *v1beta2.ResourceReservation) error {
	_, err := d.sparkschedulerV1beta2Interface.ResourceReservations(obj.GetNamespace()).Update(ctx, obj, metav1.UpdateOptions{})
	return err
}
