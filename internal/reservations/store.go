// Copyright (c) 2019 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reservations

import (
	"context"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta2"
	sparkschedulerlisters "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/listers/sparkscheduler/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Store wraps a raw reservations client and a lister to provide a single interface for reads and writes
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

// NewDefaultStore Creates the default implementation of Store
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
	result, err := d.sparkschedulerV1beta2Interface.ResourceReservations(obj.GetNamespace()).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	d.allowListerToSync(ctx, result)
	return err
}

func (d *defaultStore) Update(ctx context.Context, obj *v1beta2.ResourceReservation) error {
	result, err := d.sparkschedulerV1beta2Interface.ResourceReservations(obj.GetNamespace()).Update(ctx, obj, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	d.allowListerToSync(ctx, result)
	return err
}

func (d *defaultStore) allowListerToSync(ctx context.Context, result *v1beta2.ResourceReservation) {
	ticker := time.NewTicker(time.Millisecond * 20)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*5))
	defer ticker.Stop()
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := d.resourceReservationLister.ResourceReservations(result.Namespace).Get(result.Name)
			if err == nil && ok.Generation >= result.Generation {
				return
			}
		}
	}
}
