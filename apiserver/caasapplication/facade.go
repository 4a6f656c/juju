// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package caasapplication

import (
	"regexp"

	"github.com/juju/errors"
	"github.com/juju/loggo"
	"github.com/juju/utils/featureflag"
	"gopkg.in/juju/charm.v6-unstable"
	csparams "gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/juju/names.v2"

	"github.com/juju/juju/apiserver/common"
	"github.com/juju/juju/apiserver/facade"
	"github.com/juju/juju/apiserver/params"
	jujucrossmodel "github.com/juju/juju/core/crossmodel"
	"github.com/juju/juju/feature"
	"github.com/juju/juju/permission"
	"github.com/juju/juju/state"
)

var logger = loggo.GetLogger("juju.apiserver.caasapplication")

// NewFacade returns a new CAAS application facade.
func NewFacade(ctx facade.Context) (*Facade, error) {
	auth := ctx.Auth()
	if !auth.AuthClient() {
		return nil, common.ErrPerm
	}
	return &Facade{
		authorizer: auth,
		backend:    ctx.CAASState(),
		statePool:  ctx.StatePool(),
	}, nil
}

// Facade implements the application interface and is the concrete
// implementation of the api end point.
type Facade struct {
	authorizer facade.Authorizer
	backend    *state.CAASState
	statePool  *state.StatePool
}

func (facade *Facade) checkPermission(tag names.Tag, perm permission.Access) error {
	allowed, err := facade.authorizer.HasPermission(perm, tag)
	if err != nil {
		return errors.Trace(err)
	}
	if !allowed {
		return common.ErrPerm
	}
	return nil
}

func (facade *Facade) checkCanRead() error {
	return facade.checkPermission(facade.backend.ModelTag(), permission.ReadAccess)
}

func (facade *Facade) checkCanWrite() error {
	return facade.checkPermission(facade.backend.ModelTag(), permission.WriteAccess)
}

// Deploy fetches the charms from the charm store and deploys them
// using the specified placement directives.
// XXX CAAS - this is missing authentication checks (prototype)
func (facade *Facade) Deploy(args params.CAASApplicationsDeploy) (params.ErrorResults, error) {
	result := params.ErrorResults{
		Results: make([]params.ErrorResult, len(args.Applications)),
	}
	for i, arg := range args.Applications {
		err := deployApplication(facade.backend, arg)
		result.Results[i].Error = common.ServerError(err)
	}
	return result, nil
}

func deployApplication(backend *state.CAASState, args params.CAASApplicationDeploy) error {
	curl, err := charm.ParseURL(args.CharmURL)
	if err != nil {
		return errors.Trace(err)
	}
	if curl.Revision < 0 {
		return errors.Errorf("charm url must include revision")
	}

	ch, err := backend.Charm(curl)
	if err != nil {
		return errors.Trace(err)
	}

	if err := checkMinVersion(ch); err != nil {
		return errors.Trace(err)
	}

	var settings charm.Settings
	if len(args.ConfigYAML) > 0 {
		settings, err = ch.Config().ParseSettingsYAML([]byte(args.ConfigYAML), args.ApplicationName)
		if err != nil {
			return errors.Trace(err)
		}
	}

	_, err = backend.AddCAASApplication(state.AddCAASApplicationArgs{
		Name:     args.ApplicationName,
		Charm:    ch,
		Channel:  csparams.Channel(args.Channel),
		Settings: settings,
		NumUnits: args.NumUnits,
	})
	return errors.Trace(err)
}

// DestroyApplication removes a given set of applications.
func (facade *Facade) DestroyApplication(args params.Entities) (params.DestroyApplicationResults, error) {
	if err := facade.checkCanWrite(); err != nil {
		return params.DestroyApplicationResults{}, err
	}
	destroyApp := func(entity params.Entity) (*params.DestroyApplicationInfo, error) {
		tag, err := names.ParseApplicationTag(entity.Tag)
		if err != nil {
			return nil, err
		}
		app, err := facade.backend.CAASApplication(tag.Id())
		if err != nil {
			return nil, err
		}
		units, err := app.AllCAASUnits()
		if err != nil {
			return nil, err
		}
		if err := app.Destroy(); err != nil {
			return nil, err
		}
		var info params.DestroyApplicationInfo
		for _, unit := range units {
			info.DestroyedUnits = append(info.DestroyedUnits, params.Entity{unit.UnitTag().String()})
		}
		return &info, nil
	}
	results := make([]params.DestroyApplicationResult, len(args.Entities))
	for i, entity := range args.Entities {
		info, err := destroyApp(entity)
		if err != nil {
			results[i].Error = common.ServerError(err)
			continue
		}
		results[i].Info = info
	}
	return params.DestroyApplicationResults{results}, nil
}

// AddUnits adds a given number of units to an application.
func (facade *Facade) AddUnits(args params.AddApplicationUnits) (params.AddApplicationUnitsResults, error) {
	units, err := addApplicationUnits(facade.backend, args)
	if err != nil {
		return params.AddApplicationUnitsResults{}, errors.Trace(err)
	}
	unitNames := make([]string, len(units))
	for i, unit := range units {
		unitNames[i] = unit.Name()
	}
	return params.AddApplicationUnitsResults{Units: unitNames}, nil
}

// addApplicationUnits adds a given number of units to an application.
func addApplicationUnits(backend *state.CAASState, args params.AddApplicationUnits) ([]*state.CAASUnit, error) {
	application, err := backend.CAASApplication(args.ApplicationName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if args.NumUnits < 1 {
		return nil, errors.New("must add at least one unit")
	}
	out := make([]*state.CAASUnit, args.NumUnits)
	for i := 0; i < args.NumUnits; i++ {
		unit, err := application.AddCAASUnit()
		if err != nil {
			return nil, err
		}
		out[i] = unit
	}
	return out, nil
}

// applicationUrlEndpointParse is used to split an application url and optional
// relation name into url and relation name.
var applicationUrlEndpointParse = regexp.MustCompile("(?P<url>.*[/.][^:]*)(:(?P<relname>.*)$)?")

// AddRelation adds a relation between the specified endpoints and returns the relation info.
func (facade *Facade) AddRelation(args params.AddRelation) (params.AddRelationResults, error) {
	/*if err := api.check.ChangeAllowed(); err != nil {
		return params.AddRelationResults{}, errors.Trace(err)
	}*/

	endpoints := make([]string, len(args.Endpoints))

	// We may have a remote application passed in as the endpoint spec.
	// We'll iterate the endpoints to check.
	isRemote := false
	for i, ep := range args.Endpoints {
		endpoints[i] = ep
		// If cross model relations not enabled, ignore remote endpoints.
		if !featureflag.Enabled(feature.CrossModelRelations) {
			continue
		}

		// If the endpoint is not remote, skip it.
		// We first need to strip off any relation name
		// which may have been appended to the URL, then
		// we try parsing the URL.
		possibleURL := applicationUrlEndpointParse.ReplaceAllString(ep, "$url")
		relName := applicationUrlEndpointParse.ReplaceAllString(ep, "$relname")

		// If the URL parses, we need to look up the remote application
		// details and save to state.
		url, err := jujucrossmodel.ParseApplicationURL(possibleURL)
		if err != nil {
			// Not a URL.
			continue
		}
		// Save the remote application details into state.
		// TODO(wallyworld) - allow app name to be aliased
		alias := url.ApplicationName
		remoteApp, err := facade.processRemoteApplication(url, alias)
		if err != nil {
			return params.AddRelationResults{}, errors.Trace(err)
		}
		// The endpoint is named after the remote application name,
		// not the application name from the URL.
		endpoints[i] = remoteApp.Name()
		if relName != "" {
			endpoints[i] = remoteApp.Name() + ":" + relName
		}
		isRemote = true
	}

	// If it's not a remote relation to another model then
	// the user needs write access to the model.
	if !isRemote {
		if err := facade.checkCanWrite(); err != nil {
			return params.AddRelationResults{}, errors.Trace(err)
		}
	}

	inEps, err := facade.backend.InferEndpoints(endpoints...)
	if err != nil {
		return params.AddRelationResults{}, errors.Trace(err)
	}
	rel, err := facade.backend.AddRelation(inEps...)
	if err != nil {
		return params.AddRelationResults{}, errors.Trace(err)
	}

	outEps := make(map[string]params.CharmRelation)
	for _, inEp := range inEps {
		outEp, err := rel.Endpoint(inEp.ApplicationName)
		if err != nil {
			return params.AddRelationResults{}, errors.Trace(err)
		}
		outEps[inEp.ApplicationName] = params.CharmRelation{
			Name:      outEp.Relation.Name,
			Role:      string(outEp.Relation.Role),
			Interface: outEp.Relation.Interface,
			Optional:  outEp.Relation.Optional,
			Limit:     outEp.Relation.Limit,
			Scope:     string(outEp.Relation.Scope),
		}
	}
	return params.AddRelationResults{Endpoints: outEps}, nil
}

// Consume adds remote applications to the model without creating any
// relations.
func (facade *Facade) Consume(args params.ConsumeApplicationArgs) (params.ConsumeApplicationResults, error) {
	var consumeResults params.ConsumeApplicationResults
	/*if err := facade.check.ChangeAllowed(); err != nil {
		return consumeResults, errors.Trace(err)
	}*/
	if !featureflag.Enabled(feature.CrossModelRelations) {
		err := errors.Errorf(
			"set %q feature flag to enable consuming remote applications",
			feature.CrossModelRelations,
		)
		return consumeResults, err
	}
	results := make([]params.ConsumeApplicationResult, len(args.Args))
	for i, arg := range args.Args {
		localName, err := facade.consumeOne(arg.ApplicationURL, arg.ApplicationAlias)
		results[i].LocalName = localName
		results[i].Error = common.ServerError(err)
	}
	consumeResults.Results = results
	return consumeResults, nil
}

func (facade *Facade) consumeOne(possibleURL, alias string) (string, error) {
	url, err := jujucrossmodel.ParseApplicationURL(possibleURL)
	if err != nil {
		return "", errors.Trace(err)
	}
	if url.HasEndpoint() {
		return "", errors.Errorf("remote application %q shouldn't include endpoint", url)
	}
	remoteApp, err := facade.processRemoteApplication(url, alias)
	if err != nil {
		return "", errors.Trace(err)
	}
	return remoteApp.Name(), nil
}

func (facade *Facade) sameControllerSourceModel(userName, modelName string) (names.ModelTag, error) {
	// Look up the model by qualified name, ie user/model.
	var sourceModelTag names.ModelTag
	allIAASModels, err := facade.backend.AllIAASModels()
	if err != nil {
		return sourceModelTag, errors.Trace(err)
	}
	for _, m := range allIAASModels {
		if m.Name() != modelName {
			continue
		}
		if m.Owner().Name() != userName {
			continue
		}
		sourceModelTag = m.Tag().(names.ModelTag)
	}
	if sourceModelTag.Id() == "" {
		return sourceModelTag, errors.NotFoundf(`model "%s/%s"`, userName, modelName)
	}
	return sourceModelTag, nil
}

// processRemoteApplication takes a remote application URL and retrieves or confirms the the details
// of the application and endpoint. These details are saved to the state model so relations to
// the remote application can be created.
func (facade *Facade) processRemoteApplication(url *jujucrossmodel.ApplicationURL, alias string) (*state.RemoteApplication, error) {
	app, releaser, sourceModelTag, err := facade.sameControllerOfferedApplication(url, permission.ConsumeAccess)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer releaser()

	eps, err := app.Endpoints()
	if err != nil {
		return nil, errors.Trace(err)
	}
	endpoints := make([]params.RemoteEndpoint, len(eps))
	for i, ep := range eps {
		endpoints[i] = params.RemoteEndpoint{
			Name:      ep.Name,
			Scope:     ep.Scope,
			Interface: ep.Interface,
			Role:      ep.Role,
			Limit:     ep.Limit,
		}
	}
	appName := alias
	if appName == "" {
		appName = url.ApplicationName
	}
	remoteApp, err := facade.saveRemoteApplication(sourceModelTag, appName, url.ApplicationName, url.String(), endpoints)
	return remoteApp, err
}

// sameControllerOfferedApplication looks in the specified model on the same controller
// and returns the specified application and a reference to its state.State.
// The user is required to have the specified permission on the offer.
func (facade *Facade) sameControllerOfferedApplication(url *jujucrossmodel.ApplicationURL, perm permission.Access) (
	_ *state.Application,
	releaser func(),
	sourceModelTag names.ModelTag,
	err error,
) {
	defer func() {
		if err != nil && releaser != nil {
			releaser()
		}
	}()

	fail := func(err error) (
		*state.Application,
		func(),
		names.ModelTag,
		error,
	) {
		return nil, releaser, sourceModelTag, err
	}

	// We require the hosting model to be specified.
	if url.ModelName == "" {
		return fail(errors.Errorf("missing model name in URL %q", url.String()))
	}

	// The user name is either specified in URL, or else we default to
	// the logged in user.
	userName := url.User
	if userName == "" {
		userName = facade.authorizer.GetAuthTag().Id()
	}

	// Get the hosting model from the name.
	sourceModelTag, err = facade.sameControllerSourceModel(userName, url.ModelName)
	if err != nil {
		return fail(errors.Trace(err))
	}

	// Get the backend state for the source model so we can lookup the application.
	var st *state.State
	st, releaser, err = facade.statePool.Get(sourceModelTag.Id())
	if err != nil {
		return fail(errors.Trace(err))
	}

	// For now, offer URL is matched against the specified application
	// name as seen from the consuming model.
	applicationOffers := state.NewApplicationOffers(st)
	offers, err := applicationOffers.ListOffers(
		jujucrossmodel.ApplicationOfferFilter{
			OfferName: url.ApplicationName,
		},
	)
	if err != nil {
		return fail(errors.Trace(err))
	}

	// The offers query succeeded but there were no offers matching the required offer name.
	if len(offers) == 0 {
		return fail(errors.NotFoundf("application offer %q", url.ApplicationName))
	}
	// Sanity check - this should never happen.
	if len(offers) > 1 {
		return fail(errors.Errorf("unexpected: %d matching offers for %q", len(offers), url.ApplicationName))
	}

	// Check the permissions - a user can access the offer if they are an admin
	// or they have consume access to the offer.
	isAdmin := true // XXX CAAS
	/*isAdmin := false
	err = facade.checkPermission(st.ControllerTag(), permission.SuperuserAccess)
	if err == common.ErrPerm {
		err = facade.checkPermission(sourceModelTag, permission.AdminAccess)
	}
	if err != nil && err != common.ErrPerm {
		return fail(errors.Trace(err))
	}
	isAdmin = err == nil*/

	offer := offers[0]
	if !isAdmin {
		// Check for consume access on tne offer - we can't use api.checkPermission as
		// we need to operate on the state containing the offer.
		apiUser := facade.authorizer.GetAuthTag().(names.UserTag)
		access, err := st.GetOfferAccess(names.NewApplicationOfferTag(offer.OfferName), apiUser)
		if err != nil && !errors.IsNotFound(err) {
			return fail(errors.Trace(err))
		}
		if !access.EqualOrGreaterOfferAccessThan(perm) {
			return fail(common.ErrPerm)
		}
	}
	app, err := st.Application(offer.ApplicationName)
	if err != nil {
		return fail(errors.Trace(err))
	}
	return app, releaser, sourceModelTag, err
}

// saveRemoteApplication saves the details of the specified remote application and its endpoints
// to the state model so relations to the remote application can be created.
func (facade *Facade) saveRemoteApplication(
	sourceModelTag names.ModelTag, applicationName, offerName, url string, endpoints []params.RemoteEndpoint,
) (*state.RemoteApplication, error) {
	remoteEps := make([]charm.Relation, len(endpoints))
	for j, ep := range endpoints {
		remoteEps[j] = charm.Relation{
			Name:      ep.Name,
			Role:      ep.Role,
			Interface: ep.Interface,
			Limit:     ep.Limit,
			Scope:     ep.Scope,
		}
	}

	// If the a remote application with the same name and endpoints from the same
	// source model already exists, we will use that one.
	remoteApp, err := facade.maybeUpdateExistingApplicationEndpoints(applicationName, sourceModelTag, remoteEps)
	if err == nil {
		return remoteApp, nil
	} else if !errors.IsNotFound(err) {
		return nil, errors.Trace(err)
	}

	return facade.backend.AddRemoteApplication(state.AddRemoteApplicationParams{
		Name:        applicationName,
		OfferName:   offerName,
		URL:         url,
		SourceModel: sourceModelTag,
		Endpoints:   remoteEps,
	})
}

// maybeUpdateExistingApplicationEndpoints looks for a remote application with the
// specified name and source model tag and tries to update its endpoints with the
// new ones specified. If the endpoints are compatible, the newly updated remote
// application is returned.
func (facade *Facade) maybeUpdateExistingApplicationEndpoints(
	applicationName string, sourceModelTag names.ModelTag, remoteEps []charm.Relation,
) (*state.RemoteApplication, error) {
	existingRemoteApp, err := facade.backend.RemoteApplication(applicationName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err != nil && !errors.IsNotFound(err) {
		return nil, errors.Trace(err)
	}
	if existingRemoteApp.SourceModel().Id() != sourceModelTag.Id() {
		return nil, errors.AlreadyExistsf("remote application called %q from a different model", applicationName)
	}
	newEpsMap := make(map[charm.Relation]bool)
	for _, ep := range remoteEps {
		newEpsMap[ep] = true
	}
	existingEps, err := existingRemoteApp.Endpoints()
	if err != nil {
		return nil, errors.Trace(err)
	}
	maybeSameEndpoints := len(newEpsMap) == len(existingEps)
	existingEpsByName := make(map[string]charm.Relation)
	for _, ep := range existingEps {
		existingEpsByName[ep.Name] = ep.Relation
		delete(newEpsMap, ep.Relation)
	}
	sameEndpoints := maybeSameEndpoints && len(newEpsMap) == 0
	if sameEndpoints {
		return existingRemoteApp, nil
	}

	// Gather the new endpoints. All new endpoints passed to AddEndpoints()
	// below must not have the same name as an existing endpoint.
	var newEps []charm.Relation
	for ep := range newEpsMap {
		// See if we are attempting to update endpoints with the same name but
		// different relation data.
		if existing, ok := existingEpsByName[ep.Name]; ok && existing != ep {
			return nil, errors.Errorf("conflicting endpoint %v", ep.Name)
		}
		newEps = append(newEps, ep)
	}

	if len(newEps) > 0 {
		// Update the existing remote app to have the new, additional endpoints.
		if err := existingRemoteApp.AddEndpoints(newEps); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return existingRemoteApp, nil
}

// DestroyRelation removes the relation between the specified endpoints.
func (facade *Facade) DestroyRelation(args params.DestroyRelation) error {
	if err := facade.checkCanWrite(); err != nil {
		return err
	}
	/*if err := facade.check.RemoveAllowed(); err != nil {
		return errors.Trace(err)
	}*/
	eps, err := facade.backend.InferEndpoints(args.Endpoints...)
	if err != nil {
		return err
	}
	rel, err := facade.backend.EndpointsRelation(eps...)
	if err != nil {
		return err
	}
	return rel.Destroy()
}
