package internal

import v1 "k8s.io/api/core/v1"

func FindInstanceGroupFromNodeAffinity(podSpec v1.PodSpec, instanceGroupLabel string) (instanceGroup string, success bool) {
	for _, nodeSelectorTerm := range podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, matchExpression := range nodeSelectorTerm.MatchExpressions {
			if matchExpression.Key == instanceGroupLabel {
				if len(matchExpression.Values) == 1 {
					return matchExpression.Values[0], true
				}
			}
		}
	}
	return "", false
}

