package registry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/client-go/dynamic"
	restclient "k8s.io/client-go/rest"
	transport2 "k8s.io/client-go/transport"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics"
	"k8s.io/utils/pointer"
)

type FleetClientSet struct {
	clusterGetter  func(string) (*clusterv1alpha1.Cluster, error)
	clustersLister func() ([]*clusterv1alpha1.Cluster, error)
	secretGetter   func(string, string) (*corev1.Secret, error)

	LoopbackConfig   *restclient.Config
	karmadaLocation  *url.URL
	karmadaTransport http.RoundTripper

	codec runtime.Codec

	NewFunc     func() runtime.Object
	NewListFunc func() runtime.Object
	Versioner   storage.APIObjectVersioner
}

func (f *FleetClientSet) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no request info found from ctx")
	}

	resourceName, clusterName := ParseNameFromResourceName(info.Name, false)
	if options != nil && len(options.ResourceVersion) != 0 {
		options.ResourceVersion = newMultiClusterResourceVersionFromString(options.ResourceVersion).get(clusterName)
	}

	client, err := f.resourceClientFor(clusterName, info, info.Namespace)
	if err != nil {
		return nil, err
	}
	var obj *unstructured.Unstructured
	if len(info.Subresource) != 0 {
		obj, err = client.Get(ctx, resourceName, *options, append([]string{info.Subresource}, info.PartsAfterSubresource...)...)
	} else {
		obj, err = client.Get(ctx, resourceName, *options)
	}
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	objPtr := f.NewFunc()
	if err = decode(f.codec, b, objPtr); err != nil {
		return nil, err
	}

	accessor, err := meta.Accessor(objPtr)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %v", err)
	}
	if err = f.validateMinimumResourceVersion(options.ResourceVersion, accessor.GetResourceVersion()); err != nil {
		return nil, err
	}
	setNameAndResourceVersion(accessor, clusterName)
	return objPtr, nil
}

// referenceFieldSelectors contains field selectors that refer to resources
// collected from k8s.io/kubernetes/pkg/apis/<group>/<version>/conversion.go
var referenceFieldSelectors = sets.NewString("metadata.name", "metadata.namespace", // common
	"spec.nodeName", "spec.serviceAccountName", "status.nominatedNodeName", // pod
	"involvedObject.name", "involvedObject.namespace") // event

func removeClusterNameInReferenceField(fs fields.Selector) (fields.Selector, string, error) {
	requirements := fs.Requirements()
	selectors := make([]fields.Selector, 0, len(requirements))
	var clusterName string
	var specified bool
	for _, require := range requirements {
		if referenceFieldSelectors.Has(require.Field) {
			specified = true
			var name string
			require.Value, name = ParseNameFromResourceName(require.Value, true)
			if clusterName != "" && name != "" && clusterName != name {
				return nil, "", apierrors.NewBadRequest("specifying more than one cluster name is not allowed in field selectors")
			}
			if clusterName == "" && name != "" {
				clusterName = name
			}
		}
		if require.Operator == selection.NotEquals {
			selectors = append(selectors, fields.OneTermNotEqualSelector(require.Field, require.Value))
		} else {
			selectors = append(selectors, fields.OneTermEqualSelector(require.Field, require.Value))
		}
	}
	if specified && clusterName == "" {
		clusterName = KarmadaCluster
	}
	return fields.AndSelectors(selectors...), clusterName, nil
}

func (f *FleetClientSet) ListPredicate(ctx context.Context, p storage.SelectionPredicate, options *metainternalversion.ListOptions) (runtime.Object, error) {
	// specifying resource version(match) is not allowed when using continue
	if len(options.Continue) != 0 && len(options.ResourceVersion)+len(options.ResourceVersionMatch) > 0 {
		return nil, apierrors.NewBadRequest("specifying resource version(match) is not allowed when using continue")
	}

	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no request info found from ctx")
	}

	fs, specifiedCluster, err := removeClusterNameInReferenceField(p.Field)
	if err != nil {
		return nil, err
	}

	clusters, err := f.clustersLister()
	if err != nil {
		return nil, err
	}
	skipUnready, unreadyClusters := unready(clusters)
	shouldSkipList := unionSkip{specifyCluster(specifiedCluster), skipUnready}
	if unreadyClusters.Len() != 0 {
		warning.AddWarning(ctx, "fleet-apiserver", fmt.Sprintf("skip list from following clusters [%s] since cluster not ready", strings.Join(unreadyClusters.List(), ",")))
	}

	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})
	// Since karmada-metrics-adapter may collect metrics from member clusters, we skip karmada to avoid list duplicate metrics.
	if info.APIGroup != metrics.GroupName {
		// place karmada at the head of slice
		clusters = append([]*clusterv1alpha1.Cluster{{ObjectMeta: metav1.ObjectMeta{Name: KarmadaCluster}}}, clusters...)
	}

	listOptions := &metav1.ListOptions{
		FieldSelector:        fs.String(),
		LabelSelector:        p.Label.String(), // todo: @wm775825 wo do not know how to parse cluster name from labels
		Limit:                options.Limit,
		Continue:             options.Continue,
		ResourceVersion:      options.ResourceVersion,
		ResourceVersionMatch: options.ResourceVersionMatch,
	}
	requestCluster, listOptions, mrv := prepareBeforeList(listOptions)
	responseRV := mrv.clone()
	responseContinue := multiClusterContinue{}

	resourceNotFound := sets.NewString()
	items := make([]runtime.Object, 0, int(math.Min(float64(listOptions.Limit), 1024)))
	listFunc := func(cluster *clusterv1alpha1.Cluster) (num int, cont string, err error) {
		clusterName := cluster.Name
		if requestCluster != "" && requestCluster != clusterName {
			return 0, "", nil
		}
		defer func() {
			// clear the requestContinue to allow list resources from other clusters
			requestCluster = ""
			listOptions.Continue = ""
		}()

		if listOptions.Continue != "" {
			// specifying resource version is not allowed when using continue
			listOptions.ResourceVersion = ""
		} else {
			// if no continue, resourceVersion may set
			listOptions.ResourceVersion = mrv.get(clusterName)
		}

		objList, err := f.list(ctx, clusterName, info, listOptions)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return 0, "", err
			}
			resourceNotFound.Insert(clusterName)
			objList, err = f.NewListFunc(), nil
		}
		listAccessor, err := meta.ListAccessor(objList)
		if err != nil {
			return 0, "", err
		}
		responseRV.set(clusterName, listAccessor.GetResourceVersion())

		if err = meta.EachListItem(objList, func(object runtime.Object) error {
			clone := object.DeepCopyObject()
			accessor, err := meta.Accessor(clone)
			if err != nil {
				return err
			}
			setNameAndResourceVersion(accessor, clusterName)

			items = append(items, clone)
			num++
			return nil
		}); err != nil {
			return 0, "", err
		}
		return num, listAccessor.GetContinue(), nil
	}

	if listOptions.Limit == 0 {
		// no limit, list from the specified cluster or first cluster to the end
		for _, cluster := range clusters {
			if shouldSkipList.skip(cluster.Name) {
				continue
			}
			if _, _, err = listFunc(cluster); err != nil {
				return nil, err
			}
		}
	} else {
		for index, cluster := range clusters {
			if shouldSkipList.skip(cluster.Name) {
				continue
			}
			n, cont, err := listFunc(cluster)
			if err != nil {
				return nil, err
			}
			listOptions.Limit -= int64(n)
			if listOptions.Limit <= 0 {
				// limit <= 0  =>  stop
				if cont != "" {
					// continue != ""  =>  current cluster has remaining items, return this cluster name and continue for next list.
					responseContinue.Cluster = cluster.Name
					responseContinue.Continue = cont
				} else if index < len(clusters)-1 {
					// Current cluster has no remaining items. But we don't know whether next cluster has.
					// So return the next cluster name for continue listing.
					responseContinue.Cluster = clusters[index+1].Name
				}
				// No more items remain. Break the chuck list.
				break
			}
		}
	}

	// collect resource version from all clusters(except unready) to build global view of federation
	if err = f.fillMissingClusterResourceVersion(ctx, responseRV, info, clusters, unreadyClusters); err != nil {
		return nil, err
	}
	responseContinue.RV = responseRV.String()

	if resourceNotFound.Len() != 0 {
		warning.AddWarning(ctx, "fleet-apiserver", fmt.Sprintf("skip list from following clusters [%s] since resource not found", strings.Join(resourceNotFound.List(), ",")))
	}

	result := f.NewListFunc()
	if err = meta.SetList(result, items); err != nil {
		return nil, err
	}
	accessor, err := meta.ListAccessor(result)
	if err != nil {
		return nil, err
	}
	accessor.SetResourceVersion(responseRV.String())
	accessor.SetContinue(responseContinue.String())
	return result, nil
}

func (f *FleetClientSet) fillMissingClusterResourceVersion(ctx context.Context, mcv *multiClusterResourceVersion, info *request.RequestInfo, clusters []*clusterv1alpha1.Cluster, unready sets.String) error {
	errChan := make(chan error)
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, cluster := range clusters {
		if unready.Has(cluster.Name) {
			continue
		}
		if _, ok := mcv.rvs[cluster.Name]; ok {
			continue
		}

		wg.Add(1)
		go func(cluster string) {
			defer wg.Done()
			rv, err := f.getResourceVersion(ctx, cluster, info)
			if err != nil {
				errChan <- err
				return
			}
			if rv == "" {
				return
			}

			lock.Lock()
			defer lock.Unlock()
			mcv.set(cluster, rv)
		}(cluster.Name)
	}

	waitChan := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		waitChan <- struct{}{}
	}()

	var err error
	select {
	case <-waitChan:
	case err = <-errChan:
	}
	return err
}

func (f *FleetClientSet) getResourceVersion(ctx context.Context, clusterName string, info *request.RequestInfo) (string, error) {
	obj, err := f.list(ctx, clusterName, info, &metav1.ListOptions{Limit: 1})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	listObj, err := meta.ListAccessor(obj)
	if err != nil {
		return "", err
	}
	return listObj.GetResourceVersion(), nil
}

func (f *FleetClientSet) list(ctx context.Context, clusterName string, info *request.RequestInfo, opts *metav1.ListOptions) (runtime.Object, error) {
	client, err := f.resourceClientFor(clusterName, info, info.Namespace)
	if err != nil {
		return nil, err
	}
	ret, err := client.List(ctx, *opts)
	if err != nil {
		return nil, err
	}

	listObj := f.NewListFunc()
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return nil, err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("need ptr to slice: %v", err)
	}

	for _, item := range ret.Items {
		b, err := json.Marshal(&item)
		if err != nil {
			return nil, err
		}
		obj := f.NewFunc()
		if err = decode(f.codec, b, obj); err != nil {
			return nil, err
		}
		v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
	}

	accessor, err := meta.ListAccessor(listObj)
	if err != nil {
		return nil, err
	}
	accessor.SetResourceVersion(ret.GetResourceVersion())
	accessor.SetContinue(ret.GetContinue())
	return listObj, nil
}

func (f *FleetClientSet) validateMinimumResourceVersion(minimumResourceVersion, actualResourceVersion string) error {
	if minimumResourceVersion == "" {
		return nil
	}
	minimumRV, err := f.Versioner.ParseResourceVersion(minimumResourceVersion)
	if err != nil {
		return apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}
	actualRV, err := f.Versioner.ParseResourceVersion(actualResourceVersion)
	if err != nil {
		return apierrors.NewInternalError(fmt.Errorf("invalid resource version: %v", err))
	}
	// Enforce the storage.Interface guarantee that the resource version of the returned data
	// "will be at least 'resourceVersion'".
	if minimumRV > actualRV {
		return storage.NewTooLargeResourceVersionError(minimumRV, actualRV, 0)
	}
	return nil
}

func (f *FleetClientSet) resourceClientFor(clusterName string, info *request.RequestInfo, namespace string) (dynamic.ResourceInterface, error) {
	client, err := f.dynamicClientFor(clusterName)
	if err != nil {
		return nil, err
	}
	resourceClient := client.Resource(schema.GroupVersionResource{Group: info.APIGroup, Version: info.APIVersion, Resource: info.Resource})
	if namespace != "" && info.Resource != "namespaces" {
		return resourceClient.Namespace(namespace), nil
	}
	return resourceClient, nil
}

func (f *FleetClientSet) dynamicClientFor(clusterName string) (*dynamic.DynamicClient, error) {
	restConfig, err := f.restConfigFor(clusterName)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(restConfig)
}

func (f *FleetClientSet) Location(clusterName string) (*url.URL, http.RoundTripper, error) {
	if clusterName == KarmadaCluster {
		return f.karmadaLocation, f.karmadaTransport, nil
	}

	restConfig, err := f.restConfigFor(clusterName)
	if err != nil {
		return nil, nil, err
	}
	location, err := url.Parse(restConfig.Host)
	if err != nil {
		return nil, nil, err
	}
	return normalizeLocation(location), transport2.NewBearerAuthRoundTripper(restConfig.BearerToken, restConfig.Transport), nil
}

// restClientFor return rest client in the dynamicClient
func (f *FleetClientSet) restClientFor(clusterName string) (*restclient.RESTClient, error) {
	config, err := f.restConfigFor(clusterName)
	if err != nil {
		return nil, err
	}
	config = dynamic.ConfigFor(config)
	config.GroupVersion = &schema.GroupVersion{}
	config.APIPath = "/"
	return restclient.RESTClientFor(config)
}

func (f *FleetClientSet) restConfigFor(clusterName string) (*restclient.Config, error) {
	if clusterName == KarmadaCluster {
		return f.LoopbackConfig, nil
	}

	cluster, err := f.clusterGetter(clusterName)
	if err != nil {
		return nil, InterpretGetClusterError(err, clusterName)
	}
	apiEndpoint := cluster.Spec.APIEndpoint
	if apiEndpoint == "" {
		return nil, fmt.Errorf("the api endpoint of cluster %s is empty", clusterName)
	}

	if cluster.Spec.SecretRef == nil {
		return nil, fmt.Errorf("cluster %s does not have a secret", clusterName)
	}

	secret, err := f.secretGetter(cluster.Spec.SecretRef.Namespace, cluster.Spec.SecretRef.Name)
	if err != nil {
		return nil, err
	}
	caBundle, found := secret.Data[clusterv1alpha1.SecretCADataKey]
	if !found {
		return nil, fmt.Errorf("the CA bundle of cluster %s is empty", clusterName)
	}
	token, found := secret.Data["token"]
	if !found {
		return nil, fmt.Errorf("token not found")
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caBundle)
	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: cluster.Spec.InsecureSkipTLSVerification,
	}

	var proxyDialerFn utilnet.DialFunc
	trans := utilnet.SetTransportDefaults(&http.Transport{
		DialContext:     proxyDialerFn,
		TLSClientConfig: tlsConfig,
	})
	// We don't need the default env proxy. When making cluster requests,
	// we need to determine whether to initiate a proxy request based on
	// whether the proxy is nil or not.
	// As a result, we do not support reading proxy ino from env variables.
	trans.Proxy = nil

	if proxyURL := cluster.Spec.ProxyURL; proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse url of proxy url %s: %v", proxyURL, err)
		}
		trans.Proxy = http.ProxyURL(u)
		trans.ProxyConnectHeader = ParseProxyHeaders(cluster.Spec.ProxyHeader)
	}

	return &restclient.Config{
		BearerToken: string(token),
		Host:        apiEndpoint,
		Transport:   trans,
	}, nil
}

func (f *FleetClientSet) WatchPredicate(ctx context.Context, p storage.SelectionPredicate, resourceVersion string, sendInitialEvents *bool) (watch.Interface, error) {
	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no request info found from ctx")
	}

	fs, specifiedCluster, err := removeClusterNameInReferenceField(p.Field)
	if err != nil {
		return nil, err
	}

	opts := &metav1.ListOptions{
		Watch:               true,
		FieldSelector:       fs.String(),
		LabelSelector:       p.Label.String(), // todo: @wm775825 wo do not know how to parse cluster name from labels
		SendInitialEvents:   sendInitialEvents,
		AllowWatchBookmarks: p.AllowWatchBookmarks,
	}
	if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
		opts.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
	}

	mrv := newMultiClusterResourceVersionFromString(resourceVersion)
	setObjectRVFunc := func(cluster string, event *watch.Event) {
		if event == nil {
			return
		}
		b, err := json.Marshal(event.Object)
		if err != nil {
			return
		}

		if event.Type == watch.Error {
			obj := &metav1.Status{}
			if err = decode(f.codec, b, obj); err != nil {
				status := apierrors.NewInternalError(fmt.Errorf("failed to decode error event into status: %v", err)).Status()
				obj = &status
			}
			event.Object = obj
			return
		}

		obj := f.NewFunc()
		if err = decode(f.codec, b, obj); err != nil {
			status := apierrors.NewInternalError(fmt.Errorf("failed to decode event into versioned object: %v", err)).Status()
			event.Object = &status
			return
		}
		event.Object = obj

		accessor, err := meta.Accessor(obj)
		if err != nil {
			return
		}
		setNameAndResourceVersion(accessor, cluster)
	}

	clusters, err := f.clustersLister()
	if err != nil {
		return nil, err
	}
	skipUnready, unreadyClusters := unready(clusters)
	if unreadyClusters.Len() != 0 {
		warning.AddWarning(ctx, "fleet-apiserver", fmt.Sprintf("skip watch from following clusters [%s] since cluster not ready", strings.Join(unreadyClusters.List(), ",")))
	}
	shouldSkipWatch := unionSkip{specifyCluster(specifiedCluster), skipUnready}
	// Since karmada-metrics-adapter may collect metrics from member clusters, we skip karmada to avoid watch duplicate metrics.
	if info.APIGroup != metrics.GroupName {
		clusters = append(clusters, &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: KarmadaCluster}})
	}

	resourceNotFound := sets.NewString()
	watcher := newWatchMux()
	for _, cluster := range clusters {
		name := cluster.Name
		if shouldSkipWatch.skip(name) {
			continue
		}

		opts.ResourceVersion = mrv.get(name)
		w, err := f.watcherFor(ctx, name, info, *opts)
		if err != nil {
			if apierrors.IsNotFound(err) {
				resourceNotFound.Insert(name)
				continue
			}
			return nil, err
		}
		watcher.AddSource(w, func(event *watch.Event) {
			setObjectRVFunc(name, event)
		})
	}
	if resourceNotFound.Len() != 0 {
		warning.AddWarning(ctx, "fleet-apiserver", fmt.Sprintf("skip watch from following clusters [%s] since resource not found", strings.Join(resourceNotFound.List(), ",")))
	}

	watcher.Start()
	return watcher, nil
}

func (f *FleetClientSet) watcherFor(ctx context.Context, clusterName string, info *request.RequestInfo, opts metav1.ListOptions) (w watch.Interface, err error) {
	client, err := f.resourceClientFor(clusterName, info, info.Namespace)
	if err != nil {
		return nil, err
	}
	return client.Watch(ctx, opts)
}

func (f *FleetClientSet) Create(ctx context.Context, obj runtime.Object, options *metav1.CreateOptions) (runtime.Object, error) {
	o, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, apierrors.NewInternalError(goerrors.New("failed to convert object to *unstructured.Unstructured"))
	}
	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, apierrors.NewInternalError(goerrors.New("no request info found from ctx"))
	}
	ns, ok := request.NamespaceFrom(ctx)
	if !ok {
		return nil, apierrors.NewInternalError(goerrors.New("failed to get namespace from context"))
	}

	resourceName, clusterName := ParseNameFromResourceName(o.GetName(), false)
	client, err := f.resourceClientFor(clusterName, info, ns)
	if err != nil {
		return nil, err
	}
	o.SetName(resourceName)

	var ret *unstructured.Unstructured
	if len(info.Subresource) != 0 {
		ret, err = client.Create(ctx, o, *options, append([]string{info.Subresource}, info.PartsAfterSubresource...)...)
	} else {
		ret, err = client.Create(ctx, o, *options)
	}
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(ret)
	if err != nil {
		return nil, err
	}

	retObj := f.NewFunc()
	if err = decode(f.codec, b, retObj); err != nil {
		return nil, err
	}
	accessor, err := meta.Accessor(retObj)
	if err != nil {
		return nil, err
	}
	setNameAndResourceVersion(accessor, clusterName)
	return retObj, nil
}

func (f *FleetClientSet) Delete(ctx context.Context, name string, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, false, fmt.Errorf("no request info found from ctx")
	}

	resourceName, clusterName := ParseNameFromResourceName(name, false)
	info.Name = resourceName
	if options.Preconditions != nil && options.Preconditions.ResourceVersion != nil {
		options.Preconditions.ResourceVersion = pointer.String(newMultiClusterResourceVersionFromString(*options.Preconditions.ResourceVersion).get(clusterName))
	}

	body, err := encodeMetav1OptionsIntoJsonBytes(options, "DeleteOptions")
	if err != nil {
		return nil, false, err
	}

	client, err := f.restClientFor(clusterName)
	if err != nil {
		return nil, false, err
	}
	result := client.Delete().
		AbsPath(constructURLPath(&url.URL{}, info)).
		SetHeader("Content-Type", runtime.ContentTypeJSON).
		Body(body).
		Do(ctx)
	if err = result.Error(); err != nil {
		return nil, false, err
	}
	obj, err := result.Get()
	if err != nil {
		return nil, false, err
	}
	return obj, true, nil
}

func (f *FleetClientSet) Update(ctx context.Context, objInfo rest.UpdatedObjectInfo, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, false, fmt.Errorf("no request info found from ctx")
	}

	resourceName, clusterName := ParseNameFromResourceName(info.Name, false)

	client, err := f.resourceClientFor(clusterName, info, info.Namespace)
	if err != nil {
		return nil, false, err
	}

	req, ok := request.RequestFrom(ctx)
	if !ok {
		return nil, false, fmt.Errorf("no http request found from ctx")
	}

	var ret *unstructured.Unstructured
	switch req.Method {
	case http.MethodPut:
		newObj, err := objInfo.UpdatedObject(ctx, nil)
		if err != nil {
			klog.Errorf("failed to get new object with objInfo.UpdatedObject: %v", err)
			return nil, false, err
		}

		obj, ok := newObj.(*unstructured.Unstructured)
		if !ok {
			return nil, false, apierrors.NewInternalError(goerrors.New("failed to convert object to *unstructured.Unstructured"))
		}

		obj.SetName(resourceName)
		obj.SetResourceVersion(newMultiClusterResourceVersionFromString(obj.GetResourceVersion()).get(clusterName))

		if len(info.Subresource) != 0 {
			ret, err = client.Update(ctx, obj, *options, append([]string{info.Subresource}, info.PartsAfterSubresource...)...)
		} else {
			ret, err = client.Update(ctx, obj, *options)
		}
		if err != nil {
			klog.Errorf("update err: %v", err)
			return nil, false, err
		}
	case http.MethodPatch:
		newObj, err := objInfo.UpdatedObject(ctx, nil)
		if err != nil {
			klog.Errorf("failed to get new object with objInfo.UpdatedObject: %v", err)
			return nil, false, err
		}
		patchObj := newObj.(rest.PatchObject)

		pt := req.Header.Get("Content-Type")
		if len(info.Subresource) != 0 {
			ret, err = client.Patch(ctx, resourceName, types.PatchType(pt), patchObj.GetPatched(), *createToPatchOptions(options), append([]string{info.Subresource}, info.PartsAfterSubresource...)...)
		} else {
			ret, err = client.Patch(ctx, resourceName, types.PatchType(pt), patchObj.GetPatched(), *createToPatchOptions(options))
		}
		if err != nil {
			klog.Errorf("failed to patch: %v", err)
			return nil, false, err
		}
	default:
		klog.Errorf("operation %s not support now", req.Method)
		return nil, false, fmt.Errorf("operation %s not support now", req.Method)
	}

	b, err := json.Marshal(ret)
	if err != nil {
		klog.Errorf("failed to marshal: %v", err)
		return nil, false, err
	}

	objPtr := f.NewFunc()
	if err = decode(f.codec, b, objPtr); err != nil {
		klog.Errorf("failed to decode object: %v", err)
		return nil, false, err
	}
	accessor, err := meta.Accessor(objPtr)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get metadata: %v", err)
	}
	setNameAndResourceVersion(accessor, clusterName)
	return objPtr, false, nil
}

func createToPatchOptions(uo *metav1.UpdateOptions) *metav1.PatchOptions {
	if uo == nil {
		return nil
	}
	po := &metav1.PatchOptions{
		DryRun:          uo.DryRun,
		FieldManager:    uo.FieldManager,
		FieldValidation: uo.FieldValidation,
	}
	po.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("PatchOptions"))
	return po
}
