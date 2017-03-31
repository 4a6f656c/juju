// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package state

import (
	"github.com/juju/errors"
	"github.com/juju/juju/state/storage"
	"github.com/juju/juju/state/watcher"
	"github.com/juju/utils"
	"github.com/juju/utils/clock"
	"gopkg.in/juju/charm.v6-unstable"
	csparams "gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/juju/names.v2"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/txn"
)

type CAASState struct {
	modelTag      names.ModelTag
	controllerTag names.ControllerTag
	session       *mgo.Session
	database      Database
	clock         clock.Clock
}

func newCAASState(
	parentSt *State,
	modelTag names.ModelTag,
	clock clock.Clock,
) (_ *CAASState, err error) {
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

func (st *CAASState) db() Database {
	return st.database
}

func (st *CAASState) txnLogWatcher() *watcher.Watcher {
	panic("not implemented")
}

func (st *CAASState) newStorage() storage.Storage {
	return storage.NewStorage(st.modelTag.Id(), st.session)
}

func (st *CAASState) start() error {
	// XXX workers
	return nil
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

func (st *CAASState) ModelUUID() string {
	return st.modelTag.Id()
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

func (st *CAASState) Close() error {
	// XXX child worker shutdown here
	st.session.Close()
	return nil
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

	if exists, err := isNotDead(st, applicationsC, args.Name); err != nil {
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
			if exists, err := isNotDead(st, applicationsC, args.Name); err != nil {
				return nil, errors.Trace(err)
			} else if exists {
				return nil, errLocalApplicationExists
			}
		}
		// The addCAASApplicationOps does not include the model alive assertion,
		// so we add it here.
		ops := []txn.Op{
			assertModelActiveOp(st.ModelUUID()),
		}
		addOps, err := addCAASApplicationOps(st, addCAASApplicationOpsArgs{
			appDoc:   appDoc,
			settings: map[string]interface{}(args.Settings),
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
		ops = append(ops, addOps...)
		return ops, nil
	}
	if err = st.db().Run(buildTxn); err != nil {
		return nil, errors.Trace(err)
	}

	// Refresh to pick the txn-revno.
	if err = app.Refresh(); err != nil {
		return nil, errors.Trace(err)
	}
	return app, nil
}
