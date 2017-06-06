// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package state

import (
	"fmt"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/juju/utils"
	"github.com/juju/utils/clock"
	"gopkg.in/juju/charm.v6-unstable"
	csparams "gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/juju/names.v2"
	"gopkg.in/juju/worker.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"

	"github.com/juju/juju/environs/config"
	"github.com/juju/juju/state/storage"
	"github.com/juju/juju/state/watcher"
	"github.com/juju/juju/status"
)

type CAASState struct {
	modelTag      names.ModelTag
	controllerTag names.ControllerTag
	session       *mgo.Session
	database      Database
	workers       *caasWorkers
	clock         clock.Clock
}

func newCAASState(parentSt *State, modelTag names.ModelTag, clock clock.Clock) (_ *CAASState, err error) {
	session := parentSt.MongoSession().Copy()

	defer func() {
		if err != nil {
			session.Close()
		}
	}()

	rawDB := session.DB(jujuDB)
	database, err := allCollections().Load(rawDB, modelTag.Id(), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &CAASState{
		modelTag:      modelTag,
		controllerTag: parentSt.ControllerTag(),
		session:       session,
		database:      database,
		clock:         clock,
	}, nil
}

func (st *CAASState) modelClock() clock.Clock {
	return st.clock
}

func (st *CAASState) db() Database {
	return st.database
}

func (st *CAASState) txnLogWatcher() *watcher.Watcher {
	return st.workers.txnLogWatcher()
}

func (st *CAASState) newStorage() storage.Storage {
	return storage.NewStorage(st.modelTag.Id(), st.session)
}

func (st *CAASState) start() error {
	ws, err := newCAASWorkers(st)
	if err != nil {
		return errors.Trace(err)
	}
	st.workers = ws
	return nil
}

func (st *CAASState) uuid() string {
	return st.ModelUUID()
}

// XXX extract to dbIdMapper and embedded into State and CAAS state
func (st *CAASState) docID(localID string) string {
	return ensureModelUUID(st.ModelUUID(), localID)
}

// XXX
func (st *CAASState) localID(ID string) string {
	modelUUID, localID, ok := splitDocID(ID)
	if !ok || modelUUID != st.ModelUUID() {
		return ID
	}
	return localID
}

// XXX
func (st *CAASState) strictLocalID(ID string) (string, error) {
	modelUUID, localID, ok := splitDocID(ID)
	if !ok || modelUUID != st.ModelUUID() {
		return "", errors.Errorf("unexpected id: %#v", ID)
	}
	return localID, nil
}

func (st *CAASState) ControllerUUID() string {
	return st.controllerTag.Id()
}

func (st *CAASState) ControllerTag() names.ControllerTag {
	return st.controllerTag
}

func (st *CAASState) ModelUUID() string {
	return st.modelTag.Id()
}

func (st *CAASState) ModelTag() names.ModelTag {
	return st.modelTag
}

func (st *CAASState) AllIAASModels() ([]*Model, error) {
	models, closer := st.db().GetCollection(modelsC)
	defer closer()

	var modelDocs []modelDoc
	err := models.Find(nil).Sort("name", "owner").All(&modelDocs)
	if err != nil {
		return nil, err
	}

	result := make([]*Model, len(modelDocs))
	for i, doc := range modelDocs {
		// XXX CAAS these do not have st set, so can only be
		// queried, not manipulated.
		result[i] = &Model{doc: doc}
	}

	return result, nil
}

func (st *CAASState) CAASModel() (*CAASModel, error) {
	models, closer := st.database.GetCollection(caasModelsC)
	defer closer()

	model := &CAASModel{st: st}
	if err := model.refresh(models.FindId(st.ModelUUID())); err != nil {
		return nil, errors.Trace(err)
	}
	return model, nil
}

func (st *CAASState) MongoSession() *mgo.Session {
	return st.session
}

func (st *CAASState) Close() error {
	if st.workers != nil {
		if err := worker.Stop(st.workers); err != nil {
			return errors.Annotatef(err, "failed to stop workers")
		}
	}
	st.session.Close()
	return nil
}

// FindEntity returns the entity with the given tag.
//
// The returned value can be of type *User or *Application, depending
// on the tag.
func (st *CAASState) FindEntity(tag names.Tag) (Entity, error) {
	id := tag.Id()
	switch tag := tag.(type) {
	case names.UserTag:
		return st.User(tag)
	case names.ApplicationTag:
		return st.CAASApplication(id)
	default:
		return nil, errors.Errorf("unsupported tag %T", tag)
	}
}

// modelSetupOps returns the transactions necessary to set up a CAAS model.
func (st *CAASState) modelSetupOps(modelUUID, controllerUUID string, args CAASModelArgs) ([]txn.Op, error) {
	/* XXX
	// Inc ref count for hosted models.
	if controllerModelUUID != modelUUID {
		ops = append(ops, incHostedModelCountOp())
	}
	*/
	return []txn.Op{
		createCAASModelOp(
			args.Owner, args.Name, modelUUID, controllerUUID,
			args.Endpoint, args.CertData, args.KeyData, args.CAData),
		createUniqueOwnerModelNameOp(args.Owner, args.Name),
	}, nil
}

// createCAASModelOp returns the operation needed to create
// a caasModel document with the given name and UUID.
func createCAASModelOp(
	owner names.UserTag,
	name, uuid, controllerUUID string,
	endpoint string,
	certData, keyData, caData []byte,
) txn.Op {
	doc := &caasModelDoc{
		UUID:           uuid,
		Name:           name,
		Life:           Alive,
		Owner:          owner.Id(),
		ControllerUUID: controllerUUID,
		Endpoint:       endpoint,
		CertData:       certData,
		KeyData:        keyData,
		CAData:         caData,
	}
	return txn.Op{
		C:      caasModelsC,
		Id:     uuid,
		Assert: txn.DocMissing,
		Insert: doc,
	}
}

type AddCAASApplicationArgs struct {
	Name     string
	Charm    *Charm
	Channel  csparams.Channel
	Settings charm.Settings
	NumUnits int
}

// AddCAASApplication creates a new CAAS application, running the
// supplied charm, with the supplied name (which must be unique).
func (st *CAASState) AddCAASApplication(args AddCAASApplicationArgs) (_ *CAASApplication, err error) {
	defer errors.DeferredAnnotatef(&err, "cannot add application %q", args.Name)
	// Sanity checks.
	if !names.IsValidApplication(args.Name) {
		return nil, errors.Errorf("invalid name")
	}
	if args.Charm == nil {
		return nil, errors.Errorf("charm is nil")
	}

	if err := validateCharmVersion(args.Charm); err != nil {
		return nil, errors.Trace(err)
	}

	if exists, err := isNotDead(st, caasApplicationsC, args.Name); err != nil {
		return nil, errors.Trace(err)
	} else if exists {
		return nil, errors.Errorf("application already exists")
	}
	/* XXX
	if err := checkModelActive(st); err != nil {
		return nil, errors.Trace(err)
	}
	*/

	// The doc defaults to CharmModifiedVersion = 0, which is correct, since it
	// has, by definition, at its initial state.
	appDoc := &caasApplicationDoc{
		DocID:        args.Name,
		Name:         args.Name,
		ModelUUID:    st.ModelUUID(),
		CharmURL:     args.Charm.URL(),
		Channel:      string(args.Channel),
		Life:         Alive,
		PasswordHash: utils.AgentPasswordHash("password"), // XXX just for the prototype!
	}

	app := newCAASApplication(st, appDoc)

	statusDoc := statusDoc{
		ModelUUID:  st.ModelUUID(),
		Status:     status.Waiting,
		StatusInfo: status.MessageWaitForMachine, // XXX caas todo - review messages
		Updated:    st.clock.Now().UnixNano(),
		NeverSet:   true,
	}

	buildTxn := func(attempt int) ([]txn.Op, error) {
		// If we've tried once already and failed, check that
		// model may have been destroyed.
		if attempt > 0 {
			/* XXX
			if err := checkModelActive(st); err != nil {
				return nil, errors.Trace(err)
			}
			*/
			// Ensure a local application with the same name doesn't exist.
			if exists, err := isNotDead(st, caasApplicationsC, args.Name); err != nil {
				return nil, errors.Trace(err)
			} else if exists {
				return nil, errLocalApplicationExists
			}
		}
		// The addCAASApplicationOps does not include the model alive assertion,
		// so we add it here.
		ops := []txn.Op{
			assertCAASModelActiveOp(st.ModelUUID()),
		}
		addOps, err := addCAASApplicationOps(st, addCAASApplicationOpsArgs{
			appDoc:   appDoc,
			settings: map[string]interface{}(args.Settings),
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
		ops = append(ops, addOps...)

		// Collect unit-adding operations.
		for x := 0; x < args.NumUnits; x++ {
			_, unitOps, err := app.addApplicationUnitOps(caasApplicationAddCAASUnitOpsArgs{})
			if err != nil {
				return nil, errors.Trace(err)
			}
			ops = append(ops, unitOps...)
		}

		return ops, nil
	}

	// At the last moment before inserting the application, prime status history.
	probablyUpdateStatusHistory(st, app.globalKey(), statusDoc)

	if err := st.db().Run(buildTxn); err != nil {
		return nil, errors.Trace(err)
	}

	// Refresh to pick the txn-revno.
	if err := app.Refresh(); err != nil {
		return nil, errors.Trace(err)
	}
	return app, nil
}

// ModelConfig returns the complete config for the model represented
// by this state.
func (st *CAASState) ModelConfig() (*config.Config, error) {
	modelSettings, err := readSettings(st, settingsC, modelGlobalKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return config.New(config.NoDefaults, modelSettings.Map())
}

// User returns the state User for the given name.
func (st *CAASState) User(tag names.UserTag) (*User, error) {
	return getUser(st, tag)
}

func (st *CAASState) LatestPlaceholderCharm(curl *charm.URL) (*Charm, error) {
	return latestPlaceholderCharm(st, curl)
}

func (st *CAASState) UpdateUploadedCharm(info CharmInfo) (*Charm, error) {
	return updateUploadedCharm(st, info)
}

func (st *CAASState) PrepareLocalCharmUpload(curl *charm.URL) (chosenURL *charm.URL, err error) {
	return prepareLocalCharmUpload(st, curl)
}

func (st *CAASState) PrepareStoreCharmUpload(curl *charm.URL) (*Charm, error) {
	return prepareStoreCharmUpload(st, curl)
}

// AllCharms returns all charms in state.
func (st *CAASState) AllCharms() ([]*Charm, error) {
	return allCharms(st)
}

// Charm returns the charm with the given URL. Charms pending upload
// to storage and placeholders are never returned.
func (st *CAASState) Charm(curl *charm.URL) (*Charm, error) {
	return loadCharm(st, curl)
}

// CAASApplication returns a caasapplication state by name.
func (st *CAASState) CAASApplication(name string) (_ *CAASApplication, err error) {
	applications, closer := st.db().GetCollection(caasApplicationsC)
	defer closer()

	if !names.IsValidApplication(name) {
		return nil, errors.Errorf("%q is not a valid application name", name)
	}
	sdoc := &caasApplicationDoc{}
	err = applications.FindId(name).One(sdoc)
	if err == mgo.ErrNotFound {
		return nil, errors.NotFoundf("application %q", name)
	}
	if err != nil {
		return nil, errors.Annotatef(err, "cannot get application %q", name)
	}
	return newCAASApplication(st, sdoc), nil
}

// CAASUnit returns a unit by name.
func (st *CAASState) CAASUnit(name string) (*CAASUnit, error) {
	if !names.IsValidUnit(name) {
		return nil, errors.Errorf("%q is not a valid unit name", name)
	}
	caasUnits, closer := st.db().GetCollection(caasUnitsC)
	defer closer()

	doc := caasUnitDoc{}
	err := caasUnits.FindId(name).One(&doc)
	if err == mgo.ErrNotFound {
		return nil, errors.NotFoundf("unit %q", name)
	}
	if err != nil {
		return nil, errors.Annotatef(err, "cannot get unit %q", name)
	}
	return newCAASUnit(st, &doc), nil
}

// AllCAASApplications returns all deployed CAAS applications in the model.
func (st *CAASState) AllCAASApplications() ([]*CAASApplication, error) {
	applications, closer := st.db().GetCollection(caasApplicationsC)
	defer closer()

	sdocs := []caasApplicationDoc{}
	if err := applications.Find(bson.D{}).All(&sdocs); err != nil {
		return nil, errors.Errorf("cannot get all applications")
	}
	var caasApps []*CAASApplication
	for _, v := range sdocs {
		caasApps = append(caasApps, newCAASApplication(st, &v))
	}
	return caasApps, nil
}

// InferEndpoints returns the endpoints corresponding to the supplied names.
// There must be 1 or 2 supplied names, of the form <application>[:<relation>].
// If the supplied names uniquely specify a possible relation, or if they
// uniquely specify a possible relation once all implicit relations have been
// filtered, the endpoints corresponding to that relation will be returned.
func (st *CAASState) InferEndpoints(names ...string) ([]Endpoint, error) {
	// Collect all possible sane endpoint lists.
	var candidates [][]Endpoint
	switch len(names) {
	case 1:
		eps, err := st.endpoints(names[0], isPeer)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, ep := range eps {
			candidates = append(candidates, []Endpoint{ep})
		}
	case 2:
		eps1, err := st.endpoints(names[0], notPeer)
		if err != nil {
			return nil, errors.Trace(err)
		}
		eps2, err := st.endpoints(names[1], notPeer)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, ep1 := range eps1 {
			for _, ep2 := range eps2 {
				if ep1.CanRelateTo(ep2) {
					candidates = append(candidates, []Endpoint{ep1, ep2})
				}
			}
		}
	default:
		return nil, errors.Errorf("cannot relate %d endpoints", len(names))
	}
	// If there's ambiguity, try discarding implicit relations.
	switch len(candidates) {
	case 0:
		return nil, errors.Errorf("no relations found")
	case 1:
		return candidates[0], nil
	}
	var filtered [][]Endpoint
outer:
	for _, cand := range candidates {
		for _, ep := range cand {
			if ep.IsImplicit() {
				continue outer
			}
		}
		filtered = append(filtered, cand)
	}
	if len(filtered) == 1 {
		return filtered[0], nil
	}
	keys := []string{}
	for _, cand := range candidates {
		keys = append(keys, fmt.Sprintf("%q", relationKey(cand)))
	}
	sort.Strings(keys)
	return nil, errors.Errorf("ambiguous relation: %q could refer to %s",
		strings.Join(names, " "), strings.Join(keys, "; "))
}

func caasApplicationByName(st *CAASState, name string) (ApplicationEntity, error) {
	s, err := st.RemoteApplication(name)
	if err == nil {
		return s, nil
	} else if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	return st.CAASApplication(name)
}

// endpoints returns all endpoints that could be intended by the
// supplied endpoint name, and which cause the filter param to
// return true.
func (st *CAASState) endpoints(name string, filter func(ep Endpoint) bool) ([]Endpoint, error) {
	var appName, relName string
	if i := strings.Index(name, ":"); i == -1 {
		appName = name
	} else if i != 0 && i != len(name)-1 {
		appName = name[:i]
		relName = name[i+1:]
	} else {
		return nil, errors.Errorf("invalid endpoint %q", name)
	}
	svc, err := caasApplicationByName(st, appName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	eps := []Endpoint{}
	if relName != "" {
		ep, err := svc.Endpoint(relName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		eps = append(eps, ep)
	} else {
		eps, err = svc.Endpoints()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	final := []Endpoint{}
	for _, ep := range eps {
		if filter(ep) {
			final = append(final, ep)
		}
	}
	return final, nil
}

// AddRelation creates a new relation with the given endpoints.
func (st *CAASState) AddRelation(eps ...Endpoint) (r *Relation, err error) {
	key := relationKey(eps)
	defer errors.DeferredAnnotatef(&err, "cannot add relation %q", key)
	// Enforce basic endpoint sanity. The epCount restrictions may be relaxed
	// in the future; if so, this method is likely to need significant rework.
	if len(eps) != 2 {
		return nil, errors.Errorf("relation must have two endpoints")
	}
	if !eps[0].CanRelateTo(eps[1]) {
		return nil, errors.Errorf("endpoints do not relate")
	}

	// Check applications are alive and do checks if one is remote.
	svc1, err := aliveCAASApplication(st, eps[0].ApplicationName)
	if err != nil {
		return nil, err
	}
	svc2, err := aliveCAASApplication(st, eps[1].ApplicationName)
	if err != nil {
		return nil, err
	}
	if svc1.IsRemote() && svc2.IsRemote() {
		return nil, errors.Errorf("cannot add relation between remote applications %q and %q", eps[0].ApplicationName, eps[1].ApplicationName)
	}
	remoteRelation := svc1.IsRemote() || svc2.IsRemote()
	if remoteRelation && (eps[0].Scope != charm.ScopeGlobal || eps[1].Scope != charm.ScopeGlobal) {
		return nil, errors.Errorf("both endpoints must be globally scoped for remote relations")
	}

	// We only get a unique relation id once, to save on roundtrips. If it's
	// -1, we haven't got it yet (we don't get it at this stage, because we
	// still don't know whether it's sane to even attempt creation).
	id := -1
	// If a application's charm is upgraded while we're trying to add a relation,
	// we'll need to re-validate application sanity.
	var doc *relationDoc
	buildTxn := func(attempt int) ([]txn.Op, error) {
		// Perform initial relation sanity check.
		if exists, err := isNotDead(st, relationsC, key); err != nil {
			return nil, errors.Trace(err)
		} else if exists {
			return nil, errors.AlreadyExistsf("relation %v", key)
		}
		// Collect per-application operations, checking sanity as we go.
		var ops []txn.Op
		for _, ep := range eps {
			svc, err := aliveCAASApplication(st, ep.ApplicationName)
			if err != nil {
				return nil, err
			}
			if svc.IsRemote() {
				ops = append(ops, txn.Op{
					C:      remoteApplicationsC,
					Id:     st.docID(ep.ApplicationName),
					Assert: bson.D{{"life", Alive}},
					Update: bson.D{{"$inc", bson.D{{"relationcount", 1}}}},
				})
			} else {
				localSvc := svc.(*CAASApplication)
				ch, _, err := localSvc.Charm()
				if err != nil {
					return nil, errors.Trace(err)
				}
				if !ep.ImplementedBy(ch) {
					return nil, errors.Errorf("%q does not implement %q", ep.ApplicationName, ep)
				}
				ops = append(ops, txn.Op{
					C:      caasApplicationsC,
					Id:     st.docID(ep.ApplicationName),
					Assert: bson.D{{"life", Alive}, {"charmurl", ch.URL()}},
					Update: bson.D{{"$inc", bson.D{{"relationcount", 1}}}},
				})
			}
		}

		// Create a new unique id if that has not already been done, and add
		// an operation to create the relation document.
		if id == -1 {
			var err error
			if id, err = sequence(st, "relation"); err != nil {
				return nil, errors.Trace(err)
			}
		}
		docID := st.docID(key)
		doc = &relationDoc{
			DocID:     docID,
			Key:       key,
			ModelUUID: st.ModelUUID(),
			Id:        id,
			Endpoints: eps,
			Life:      Alive,
		}
		ops = append(ops, txn.Op{
			C:      relationsC,
			Id:     docID,
			Assert: txn.DocMissing,
			Insert: doc,
		})
		return ops, nil
	}
	if err = st.db().Run(buildTxn); err == nil {
		return &Relation{st, *doc}, nil
	}
	return nil, errors.Trace(err)
}

func aliveCAASApplication(st *CAASState, name string) (ApplicationEntity, error) {
	app, err := caasApplicationByName(st, name)
	if errors.IsNotFound(err) {
		return nil, errors.Errorf("application %q does not exist", name)
	} else if err != nil {
		return nil, errors.Trace(err)
	} else if app.Life() != Alive {
		return nil, errors.Errorf("application %q is not alive", name)
	}
	return app, err
}

// EndpointsRelation returns the existing relation with the given endpoints.
func (st *CAASState) EndpointsRelation(endpoints ...Endpoint) (*Relation, error) {
	return st.KeyRelation(relationKey(endpoints))
}

// KeyRelation returns the existing relation with the given key (which can
// be derived unambiguously from the relation's endpoints).
func (st *CAASState) KeyRelation(key string) (*Relation, error) {
	return keyRelation(st, key)
}

// AllRelations returns all relations in the model ordered by id.
func (st *CAASState) AllRelations() (relations []*Relation, err error) {
	return allRelations(st)
}
