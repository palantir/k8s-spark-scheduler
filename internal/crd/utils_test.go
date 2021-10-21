package crd

import (
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"testing"
)

func Test_verifyCRD(t *testing.T) {
	type args struct {
		existing *v1.CustomResourceDefinition
		desired  *v1.CustomResourceDefinition
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
		{
			name: "Identical CRDs verify successfully.",
			args: args{
				existing: v1beta2.ResourceReservationCustomResourceDefinition(nil, v1beta1.ResourceReservationCustomResourceDefinitionVersion()),
				desired:  v1beta2.ResourceReservationCustomResourceDefinition(nil, v1beta1.ResourceReservationCustomResourceDefinitionVersion()),
			},
			want: true,
		},
		{
			name: "Different CRDs do not verify.",
			args: args{
				existing: v1beta1.ResourceReservationCustomResourceDefinition(),
				desired:  v1beta2.ResourceReservationCustomResourceDefinition(nil),
			},
			want: false,
		},
		{
			name: "Newer CRDs with the existing CRD as an additional version do not verify.",
			args: args{
				existing: v1beta1.ResourceReservationCustomResourceDefinition(),
				desired:  v1beta2.ResourceReservationCustomResourceDefinition(nil, v1beta1.ResourceReservationCustomResourceDefinitionVersion()),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verifyCRD(tt.args.existing, tt.args.desired); got != tt.want {
				t.Errorf("verifyCRD() = %v, want %v", got, tt.want)
			}
		})
	}
}
