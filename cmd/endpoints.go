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

package cmd

import (
	"encoding/json"
	"net/http"

	"github.com/palantir/k8s-spark-scheduler/internal/extender"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-server/rest"
	"github.com/palantir/witchcraft-go-server/wrouter"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
)

func registerExtenderEndpoints(r wrouter.Router, sparkSchedulerExtender *extender.SparkSchedulerExtender) error {
	if err := r.Post("/predicates", http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		decoder := json.NewDecoder(req.Body)
		var args schedulerapi.ExtenderArgs
		err := decoder.Decode(&args)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
		} else {
			rest.WriteJSONResponse(rw, sparkSchedulerExtender.Predicate(req.Context(), args), http.StatusOK)
		}
	})); err != nil {
		return werror.Wrap(err, "failed to register handler")
	}
	return nil
}
