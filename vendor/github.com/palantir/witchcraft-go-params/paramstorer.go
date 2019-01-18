package wparams

// ParamStorer is a type that stores safe and unsafe parameters. Keys should be unique across both SafeParams and
// UnsafeParams (that is, if a key occurs in one map, it should not occur in the other).
type ParamStorer interface {
	SafeParams() map[string]interface{}
	UnsafeParams() map[string]interface{}
}

// NewParamStorer returns a new ParamStorer that stores all of the params in the provided ParamStorer inputs. The params
// are added from the param storers in the order in which they are provided, and for each individual param storer all of
// the safe params are added before the unsafe params while maintaining key uniqueness across both safe and unsafe
// parameters. This means that, if the same parameter is provided by multiple ParamStorer inputs, the returned
// ParamStorer will have the key (including safe/unsafe type) and value as provided by the last ParamStorer (for
// example, if an unsafe key/value pair is provided by one ParamStorer and a later ParamStorer specifies a safe
// key/value pair with the same key, the returned ParamStorer will store the last safe key/value pair).
func NewParamStorer(paramStorers ...ParamStorer) ParamStorer {
	safeParams := make(map[string]interface{})
	unsafeParams := make(map[string]interface{})
	for _, storer := range paramStorers {
		if storer == nil {
			continue
		}
		for k, v := range storer.SafeParams() {
			safeParams[k] = v
			delete(unsafeParams, k)
		}
		for k, v := range storer.UnsafeParams() {
			unsafeParams[k] = v
			delete(safeParams, k)
		}
	}
	return NewSafeAndUnsafeParamStorer(safeParams, unsafeParams)
}

// NewSafeParamStorer returns a new ParamStorer that stores the provided parameters as SafeParams.
func NewSafeParamStorer(safeParams map[string]interface{}) ParamStorer {
	return NewSafeAndUnsafeParamStorer(safeParams, nil)
}

// NewUnsafeParamStorer returns a new ParamStorer that stores the provided parameters as UnsafeParams.
func NewUnsafeParamStorer(unsafeParams map[string]interface{}) ParamStorer {
	return NewSafeAndUnsafeParamStorer(nil, unsafeParams)
}

// NewSafeAndUnsafeParamStorer returns a new ParamStorer that stores the provided safe parameters as SafeParams and the
// unsafe parameters as UnsafeParams. If the safeParams and unsafeParams have any keys in common, the key/value pairs in
// the unsafeParams will be used (the conflicting key/value pairs provided by safeParams will be ignored).
func NewSafeAndUnsafeParamStorer(safeParams, unsafeParams map[string]interface{}) ParamStorer {
	storer := &mapParamStorer{
		safeParams:   make(map[string]interface{}),
		unsafeParams: make(map[string]interface{}),
	}
	for k, v := range safeParams {
		storer.safeParams[k] = v
	}
	for k, v := range unsafeParams {
		storer.unsafeParams[k] = v
		delete(storer.safeParams, k)
	}
	return storer
}

type mapParamStorer struct {
	safeParams   map[string]interface{}
	unsafeParams map[string]interface{}
}

func (m *mapParamStorer) SafeParams() map[string]interface{} {
	return m.safeParams
}

func (m *mapParamStorer) UnsafeParams() map[string]interface{} {
	return m.unsafeParams
}
