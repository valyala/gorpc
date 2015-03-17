package gorpc

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Dispatcher struct {
	serviceMap map[string]*serviceData
}

type serviceData struct {
	service reflect.Value
	funcMap map[string]*funcData
}

type funcData struct {
	inNum  int
	outNum int
	fv     reflect.Value
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		serviceMap: make(map[string]*serviceData),
	}
}

func (d *Dispatcher) RegisterFunc(funcName string, f interface{}) {
	sd, ok := d.serviceMap[""]
	if !ok {
		sd = &serviceData{
			funcMap: make(map[string]*funcData),
		}
		d.serviceMap[""] = sd
	}

	if _, ok := sd.funcMap[funcName]; ok {
		logPanic("gorpc.Dispatcher: function %s has been already registered", funcName)
	}

	fd := &funcData{
		fv: reflect.Indirect(reflect.ValueOf(f)),
	}
	var err error
	if fd.inNum, fd.outNum, err = validateFunc(funcName, fd.fv, false); err != nil {
		logPanic("gorpc.Disaptcher: %s", err)
	}
	sd.funcMap[funcName] = fd
}

func (d *Dispatcher) RegisterService(serviceName string, service interface{}) {
	if serviceName == "" {
		logPanic("gorpc.Dispatcher: serviceName cannot be empty")
	}
	if _, ok := d.serviceMap[serviceName]; ok {
		logPanic("gorpc.Dispatcher: service with name=[%s] has been already registered", serviceName)
	}

	funcMap := make(map[string]*funcData)

	st := reflect.TypeOf(service)
	for i := 0; i < st.NumMethod(); i++ {
		mv := st.Method(i)

		if mv.PkgPath != "" {
			// skip unexported methods
			continue
		}

		funcName := serviceName + "." + mv.Name
		fd := &funcData{
			fv: mv.Func,
		}
		var err error
		if fd.inNum, fd.outNum, err = validateFunc(funcName, fd.fv, true); err != nil {
			logPanic("gorpc.Dispatcher: %s", err)
		}
		funcMap[funcName] = fd
	}

	if len(funcMap) == 0 {
		logPanic("gorpc.Dispatcher: the service %s has no methods suitable for rpc", serviceName)
	}

	d.serviceMap[serviceName] = &serviceData{
		service: reflect.ValueOf(service),
		funcMap: funcMap,
	}
}

func validateFunc(funcName string, fv reflect.Value, isMethod bool) (inNum, outNum int, err error) {
	if funcName == "" {
		err = fmt.Errorf("funcName cannot be empty")
		return
	}

	ft := fv.Type()
	if ft.Kind() != reflect.Func {
		err = fmt.Errorf("function [%s] must be a function instead of %s", funcName, ft)
		return
	}

	dt := 0
	if isMethod {
		dt = 1
	}
	inNum = ft.NumIn()
	if inNum == 2+dt {
		argt := ft.In(dt)
		if argt.Kind() != reflect.String {
			err = fmt.Errorf("unexpected type for the first argument of the function [%s]: [%s]. Expected string", funcName, argt)
			return
		}
	} else if inNum > 2+dt {
		err = fmt.Errorf("unexpected number of arguments in the function [%s]: %d. Expected 0, 1 (request) or 2 (clientAddr, request)", funcName, inNum-dt)
		return
	}

	outNum = ft.NumOut()
	if outNum == 2 {
		argt := ft.Out(1)
		if !isErrorType(argt) {
			err = fmt.Errorf("unexpected type for the second return value of the function [%s]: [%s]. Expected [%s]", funcName, argt, errt)
			return
		}
	} else if outNum > 2 {
		err = fmt.Errorf("unexpected number of return values for the function %s: %d. Expected 0, 1 (response) or 2 (response, error)", funcName, outNum)
		return
	}

	if inNum > 0 {
		if err = registerType("request", funcName, ft.In(inNum-1)); err != nil {
			return
		}
	}

	if outNum > 0 {
		respt := ft.Out(0)
		if !isErrorType(respt) {
			if err = registerType("response", funcName, ft.Out(0)); err != nil {
				return
			}
		}
	}

	return
}

func registerType(s, funcName string, t reflect.Type) error {
	if t.Kind() == reflect.Struct {
		return fmt.Errorf("%s in the function [%s] should be passed by reference, i.e. *%s", s, funcName, t)
	}
	if err := validateType(t); err != nil {
		return fmt.Errorf("%s in the function [%s] cannot contain %s", s, funcName, err)
	}

	t = removePtr(t)
	tv := reflect.New(t)
	if t.Kind() != reflect.Struct {
		tv = reflect.Indirect(tv)
	}

	switch t.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.Struct:
		RegisterType(tv.Interface())
	default:
	}

	return nil
}

func removePtr(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

var validatedTypes []*validatedType

type validatedType struct {
	t   reflect.Type
	err *error
}

func validateType(t reflect.Type) (err error) {
	t = removePtr(t)
	for _, vd := range validatedTypes {
		if vd.t == t {
			return *vd.err
		}
	}
	validatedTypes = append(validatedTypes, &validatedType{
		t:   t,
		err: &err,
	})

	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
		err = fmt.Errorf("%s. Found [%s]", t.Kind(), t)
		return
	case reflect.Array, reflect.Slice:
		if err = validateType(t.Elem()); err != nil {
			err = fmt.Errorf("%s in the %s [%s]", err, t.Kind(), t)
			return
		}
	case reflect.Map:
		if err = validateType(t.Elem()); err != nil {
			err = fmt.Errorf("%s in the value of map [%s]", err, t)
			return
		}
		if err = validateType(t.Key()); err != nil {
			err = fmt.Errorf("%s in the key of map [%s]", err, t)
			return
		}
	case reflect.Struct:
		n := 0
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath == "" {
				if err = validateType(f.Type); err != nil {
					err = fmt.Errorf("%s in the field [%s] of struct [%s]", err, f.Name, t)
					return
				}
				n++
			}
		}
		if n == 0 {
			err = fmt.Errorf("struct without exported fields [%s]", t)
			return
		}
	}

	return err
}

type dispatcherRequest struct {
	Request interface{}
	Name    string
}

type dispatcherResponse struct {
	Response interface{}
	Error    string
}

func init() {
	RegisterType(&dispatcherRequest{})
	RegisterType(&dispatcherResponse{})
}

func (d *Dispatcher) HandlerFunc() HandlerFunc {
	if len(d.serviceMap) == 0 {
		logPanic("gorpc.Dispatcher: register at least one service before calling HandlerFunc()")
	}

	serviceMap := copyServiceMap(d.serviceMap)

	return func(clientAddr string, request interface{}) interface{} {
		req, ok := request.(*dispatcherRequest)
		if !ok {
			logPanic("gorpc.Dispatcher: unsupported request type received from the client: %T", request)
		}
		return dispatchRequest(serviceMap, clientAddr, req)
	}
}

func copyServiceMap(sm map[string]*serviceData) map[string]*serviceData {
	serviceMap := make(map[string]*serviceData)
	for sk, sv := range sm {
		funcMap := make(map[string]*funcData)
		for fk, fv := range sv.funcMap {
			funcMap[fk] = fv
		}
		serviceMap[sk] = &serviceData{
			service: sv.service,
			funcMap: funcMap,
		}
	}
	return serviceMap
}

func dispatchRequest(serviceMap map[string]*serviceData, clientAddr string, req *dispatcherRequest) *dispatcherResponse {
	callName := strings.SplitN(req.Name, ".", 2)
	if len(callName) != 2 {
		return &dispatcherResponse{
			Error: fmt.Sprintf("gorpc.Dispatcher: cannot split call name into service name and method name [%s]", req.Name),
		}
	}

	serviceName, funcName := callName[0], callName[1]
	s, ok := serviceMap[serviceName]
	if !ok {
		return &dispatcherResponse{
			Error: fmt.Sprintf("gorpc.Dispatcher: unknown service name [%s]", serviceName),
		}
	}

	fd, ok := s.funcMap[funcName]
	if !ok {
		return &dispatcherResponse{
			Error: fmt.Sprintf("gorpc.Dispatcher: unknown method call [%s] for the service [%s]", funcName, serviceName),
		}
	}

	var inArgs []reflect.Value
	reqv := reflect.ValueOf(req.Request)
	if serviceName != "" {
		if fd.inNum == 2 {
			inArgs = []reflect.Value{s.service, reflect.ValueOf(clientAddr), reqv}
		} else if fd.inNum == 1 {
			inArgs = []reflect.Value{s.service, reqv}
		}
	} else {
		if fd.inNum == 2 {
			inArgs = []reflect.Value{reflect.ValueOf(clientAddr), reqv}
		} else if fd.inNum == 1 {
			inArgs = []reflect.Value{reqv}
		}
	}

	outArgs := fd.fv.Call(inArgs)

	resp := &dispatcherResponse{}

	if len(outArgs) > 2 {
		logPanic("gorpc.Dispatcher: unexpected number of out arguments when calling [%s]: %d. Should be lower than 3", req.Name, len(outArgs))
	}

	if len(outArgs) == 1 {
		if isErrorType(outArgs[0].Type()) {
			resp.Error = getErrorString(outArgs[0])
		} else {
			resp.Response = outArgs[0].Interface()
		}
	} else if len(outArgs) == 2 {
		resp.Error = getErrorString(outArgs[1])
		if resp.Error == "" {
			resp.Response = outArgs[0].Interface()
		}
	}

	return resp
}

var errt = reflect.TypeOf((*error)(nil)).Elem()

func isErrorType(t reflect.Type) bool {
	return t.Implements(errt)
}

func getErrorString(v reflect.Value) string {
	if v.IsNil() {
		return ""
	}
	return v.Interface().(error).Error()
}

type DispatcherClient struct {
	c *Client
}

func (d *Dispatcher) NewFuncClient(c *Client) *DispatcherClient {
	return &DispatcherClient{
		c: c,
	}
}

func (dc *DispatcherClient) Call(funcName string, request interface{}) (response interface{}, err error) {
	return dc.CallTimeout(funcName, request, dc.c.RequestTimeout)
}

func (dc *DispatcherClient) CallTimeout(funcName string, request interface{}, timeout time.Duration) (response interface{}, err error) {
	req := &dispatcherRequest{
		Name:    "." + funcName,
		Request: request,
	}
	respv, err := dc.c.CallTimeout(&req, timeout)
	if err != nil {
		return
	}
	resp, ok := respv.(*dispatcherResponse)
	if !ok {
		err = fmt.Errorf("gorpc.DispatcherClient: unexpected response type: %T. Expected dispatcherResponse", respv)
		return
	}
	if resp.Error != "" {
		err = fmt.Errorf("%s", resp.Error)
		return
	}
	return resp.Response, nil
}
