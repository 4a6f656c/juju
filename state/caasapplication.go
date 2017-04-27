// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package state

import (
	"strconv"

	"github.com/juju/errors"
	jujutxn "github.com/juju/txn"
	"gopkg.in/juju/charm.v6-unstable"
	csparams "gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/juju/names.v2"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"

	"github.com/juju/juju/constraints"
	"github.com/juju/juju/status"
)

// CAASApplication represents the state of an application.
type CAASApplication struct {
	st  *CAASState
	doc caasApplicationDoc
}

// caasApplicationDoc represents the internal state of a CAAS
// application in MongoDB.
type caasApplicationDoc struct {
	DocID                string     `bson:"_id"`
	Name                 string     `bson:"name"`
	ModelUUID            string     `bson:"model-uuid"`
	CharmURL             *charm.URL `bson:"charmurl"`
	Channel              string     `bson:"cs-channel"`
	CharmModifiedVersion int        `bson:"charmmodifiedversion"`
	ForceCharm           bool       `bson:"forcecharm"`
	Life                 Life       `bson:"life"`
	UnitCount            int        `bson:"unitcount"`
	RelationCount        int        `bson:"relationcount"`
	PasswordHash         string     `bson:"passwordhash"` // XXX needs to be populated: see unit code
}

func newCAASApplication(st *CAASState, doc *caasApplicationDoc) *CAASApplication {
	return &CAASApplication{
		st:  st,
		doc: *doc,
	}
}

// Name returns the application name.
func (a *CAASApplication) Name() string {
	return a.doc.Name
}

// Tag returns a name identifying the application.
// The returned name will be different from other Tag values returned by any
// other entities from the same state.
func (a *CAASApplication) Tag() names.Tag {
	return a.ApplicationTag()
}

// ApplicationTag returns the more specific ApplicationTag rather
// than the generic Tag.
// XXX it's likely we'll want a CAAS specific application tag
func (a *CAASApplication) ApplicationTag() names.ApplicationTag {
	return names.NewApplicationTag(a.Name())
}

// globalKey returns the global database key for the application.
func (a *CAASApplication) globalKey() string {
	// XXX it's likely we'll want a separate key for caas applications
	return applicationGlobalKey(a.doc.Name)
}

// settingsKey returns the charm-version-specific settings collection
// key for the application.
func (a *CAASApplication) settingsKey() string {
	// XXX here too
	return applicationSettingsKey(a.doc.Name, a.doc.CharmURL)
}

// Life returns whether the application is Alive, Dying or Dead.
func (a *CAASApplication) Life() Life {
	return a.doc.Life
}

// Destroy ensures that the CAAS application will be removed at some point.
func (a *CAASApplication) Destroy() (err error) {
	defer errors.DeferredAnnotatef(&err, "cannot destroy CAAS application %q", a)
	defer func() {
		if err == nil {
			// This is a white lie; the document might actually be removed.
			a.doc.Life = Dying
		}
	}()
	app := newCAASApplication(a.st, &a.doc)
	buildTxn := func(attempt int) ([]txn.Op, error) {
		if attempt > 0 {
			if err := app.Refresh(); errors.IsNotFound(err) {
				return nil, jujutxn.ErrNoOperations
			} else if err != nil {
				return nil, err
			}
		}
		switch ops, err := app.destroyOps(); err {
		case errRefresh:
		case errAlreadyDying:
			return nil, jujutxn.ErrNoOperations
		case nil:
			return ops, nil
		default:
			return nil, err
		}
		return nil, jujutxn.ErrTransientFailure
	}
	return a.st.db().Run(buildTxn)
}

// destroyOps returns the operations required to destroy the application.
func (a *CAASApplication) destroyOps() ([]txn.Op, error) {
	if a.doc.Life == Dying {
		return nil, errAlreadyDying
	}
	// If the application has no units, the application can also be
	// removed.
	// TODO: Also check relations.
	if a.doc.UnitCount == 0 {
		hasLastRefs := bson.D{{"life", Alive}, {"unitcount", 0}}
		removeOps, err := a.removeOps(hasLastRefs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return removeOps, nil
	}

	ops := []txn.Op{}

	// In all other cases, application removal will be handled as a consequence
	// of the removal of the last unit or relation referencing it. If any
	// relations have been removed, they'll be caught by the operations
	// collected above; but if any has been added, we need to abort and add
	// a destroy op for that relation too. In combination, it's enough to
	// check for count equality: an add/remove will not touch the count, but
	// will be caught by virtue of being a remove.
	notLastRefs := bson.D{
		{"life", Alive},
	}
	// With respect to unit count, a changing value doesn't matter, so long
	// as the count's equality with zero does not change, because all we care
	// about is that *some* unit is, or is not, keeping the application from
	// being removed: the difference between 1 unit and 1000 is irrelevant.
	if a.doc.UnitCount > 0 {
		ops = append(ops, newCleanupOp(cleanupCAASUnitsForDyingApplication, a.doc.Name))
		notLastRefs = append(notLastRefs, bson.D{{"unitcount", bson.D{{"$gt", 0}}}}...)
	} else {
		notLastRefs = append(notLastRefs, bson.D{{"unitcount", 0}}...)
	}
	return append(ops, txn.Op{
		C:      caasApplicationsC,
		Id:     a.doc.DocID,
		Assert: notLastRefs,
		Update: bson.D{{"$set", bson.D{{"life", Dying}}}},
	}), nil
}

// removeOps returns the operations required to remove the CAAS
// application. Supplied asserts will be included in the operation on
// the application document.
func (a *CAASApplication) removeOps(asserts bson.D) ([]txn.Op, error) {
	ops := []txn.Op{
		{
			C:      caasApplicationsC,
			Id:     a.doc.DocID,
			Assert: asserts,
			Remove: true,
		}, {
			C:      settingsC,
			Id:     a.settingsKey(),
			Remove: true,
		},
	}
	// Note that appCharmDecRefOps might not catch the final decref
	// when run in a transaction that decrefs more than once. In
	// this case, luckily, we can be sure that we unconditionally
	// need finalAppCharmRemoveOps; and we trust that it's written
	// such that it's safe to run multiple times.
	name := a.doc.Name
	curl := a.doc.CharmURL
	charmOps, err := appCharmDecRefOps(a.st, name, curl)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ops = append(ops, charmOps...)
	ops = append(ops, finalAppCharmRemoveOps(name, curl)...)

	globalKey := a.globalKey()
	ops = append(ops,
		removeConstraintsOp(globalKey),
		// XXX annotationRemoveOp(a.st, globalKey),
		removeStatusOp(a.st, globalKey),
		// XXX removeModelCAASApplicationRefOp(a.st, name),
	)
	return ops, nil
}

// Charm returns the application's charm and whether units should upgrade to that
// charm even if they are in an error state.
func (a *CAASApplication) Charm() (ch *Charm, _ bool, err error) {
	// We don't worry about the channel since we aren't interacting
	// with the charm store here.
	ch, err = loadCharm(a.st, a.doc.CharmURL)
	if err != nil {
		return nil, false, err
	}
	return ch, a.doc.ForceCharm, nil
}

// CharmModifiedVersion increases whenever the application's charm is changed in any
// way.
func (a *CAASApplication) CharmModifiedVersion() int {
	return a.doc.CharmModifiedVersion
}

// CharmURL returns the application's charm URL, and whether units should upgrade
// to the charm with that URL even if they are in an error state.
func (a *CAASApplication) CharmURL() (curl *charm.URL, force bool) {
	return a.doc.CharmURL, a.doc.ForceCharm
}

// Channel identifies the charm store channel from which the application's
// charm was deployed. It is only needed when interacting with the charm
// store.
func (a *CAASApplication) Channel() csparams.Channel {
	return csparams.Channel(a.doc.Channel)
}

// changeCharmOps returns the operations necessary to set a application's
// charm URL to a new value.
func (a *CAASApplication) changeCharmOps(
	ch *Charm,
	channel string,
	updatedSettings charm.Settings,
	forceUnits bool,
) ([]txn.Op, error) {
	// Build the new application config from what can be used of the old one.
	// XXX extract
	var newSettings charm.Settings
	oldSettings, err := readSettings(a.st, settingsC, a.settingsKey())
	if err == nil {
		// Filter the old settings through to get the new settings.
		newSettings = ch.Config().FilterSettings(oldSettings.Map())
		for k, v := range updatedSettings {
			newSettings[k] = v
		}
	} else if errors.IsNotFound(err) {
		// No old settings, start with the updated settings.
		newSettings = updatedSettings
	} else {
		return nil, errors.Trace(err)
	}

	// Create or replace application settings.
	var settingsOp txn.Op
	newSettingsKey := applicationSettingsKey(a.doc.Name, ch.URL())
	if _, err := readSettings(a.st, settingsC, newSettingsKey); errors.IsNotFound(err) {
		// No settings for this key yet, create it.
		settingsOp = createSettingsOp(settingsC, newSettingsKey, newSettings)
	} else if err != nil {
		return nil, errors.Trace(err)
	} else {
		// Settings exist, just replace them with the new ones.
		settingsOp, _, err = replaceSettingsOp(a.st, settingsC, newSettingsKey, newSettings)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Add or create a reference to the new charm, settings,
	// and storage constraints docs.
	incOps, err := appCharmIncRefOps(a.st, a.doc.Name, ch.URL(), true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var decOps []txn.Op
	// Drop the references to the old settings, storage constraints,
	// and charm docs (if the refs actually exist yet).
	if oldSettings != nil {
		decOps, err = appCharmDecRefOps(a.st, a.doc.Name, a.doc.CharmURL) // current charm
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Build the transaction.
	var ops []txn.Op
	if oldSettings != nil {
		// Old settings shouldn't change (when they exist).
		ops = append(ops, oldSettings.assertUnchangedOp())
	}
	ops = append(ops, incOps...)
	ops = append(ops, []txn.Op{
		// Create or replace new settings.
		settingsOp,
		// Update the charm URL and force flag (if relevant).
		{
			C:  caasApplicationsC,
			Id: a.doc.DocID,
			Update: bson.D{{"$set", bson.D{
				{"charmurl", ch.URL()},
				{"cs-channel", channel},
				{"forcecharm", forceUnits},
			}}},
		},
	}...)
	ops = append(ops, incCAASCharmModifiedVersionOps(a.doc.DocID)...)

	// And finally, decrement the old charm and settings.
	return append(ops, decOps...), nil
}

// incCAASCharmModifiedVersionOps returns the operations necessary to increment
// the CharmModifiedVersion field for the given application.
func incCAASCharmModifiedVersionOps(applicationID string) []txn.Op {
	return []txn.Op{{
		C:      caasApplicationsC,
		Id:     applicationID,
		Assert: txn.DocExists,
		Update: bson.D{{"$inc", bson.D{{"charmmodifiedversion", 1}}}},
	}}
}

// SetCharm changes the charm for the application.
func (a *CAASApplication) SetCharm(cfg SetCharmConfig) (err error) {
	defer errors.DeferredAnnotatef(
		&err, "cannot upgrade application %q to charm %q", a, cfg.Charm,
	)

	updatedSettings, err := cfg.Charm.Config().ValidateSettings(cfg.ConfigSettings)
	if err != nil {
		return errors.Annotate(err, "validating config settings")
	}

	var newCharmModifiedVersion int
	channel := string(cfg.Channel)
	acopy := &CAASApplication{a.st, a.doc}
	buildTxn := func(attempt int) ([]txn.Op, error) {
		a := acopy
		if attempt > 0 {
			if err := a.Refresh(); err != nil {
				return nil, errors.Trace(err)
			}
		}

		// NOTE: We're explicitly allowing SetCharm to succeed
		// when the application is Dying, because application/charm
		// upgrades should still be allowed to apply to dying
		// applications and units, so that bugs in departed/broken
		// hooks can be addressed at runtime.
		if a.Life() == Dead {
			return nil, ErrDead
		}

		// Record the current value of charmModifiedVersion, so we can
		// set the value on the method receiver's in-memory document
		// structure. We increment the version only when we change the
		// charm URL.
		newCharmModifiedVersion = a.doc.CharmModifiedVersion

		ops := []txn.Op{{
			C:  caasApplicationsC,
			Id: a.doc.DocID,
			Assert: append(notDeadDoc, bson.DocElem{
				"charmmodifiedversion", a.doc.CharmModifiedVersion,
			}),
		}}

		if a.doc.CharmURL.String() == cfg.Charm.URL().String() {
			// Charm URL already set; just update the force flag and channel.
			ops = append(ops, txn.Op{
				C:  caasApplicationsC,
				Id: a.doc.DocID,
				Update: bson.D{{"$set", bson.D{
					{"cs-channel", channel},
					{"forcecharm", cfg.ForceUnits},
				}}},
			})
		} else {
			chng, err := a.changeCharmOps(
				cfg.Charm,
				channel,
				updatedSettings,
				cfg.ForceUnits,
			)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ops = append(ops, chng...)
			newCharmModifiedVersion++
		}

		return ops, nil
	}
	if err := a.st.db().Run(buildTxn); err != nil {
		return err
	}
	a.doc.CharmURL = cfg.Charm.URL()
	a.doc.Channel = channel
	a.doc.ForceCharm = cfg.ForceUnits
	a.doc.CharmModifiedVersion = newCharmModifiedVersion
	return nil
}

// String returns the application name.
func (a *CAASApplication) String() string {
	return a.doc.Name
}

// Refresh refreshes the contents of the CAASApplication from the underlying
// state. It returns an error that satisfies errors.IsNotFound if the
// application has been removed.
func (a *CAASApplication) Refresh() error {
	applications, closer := a.st.db().GetCollection(caasApplicationsC)
	defer closer()

	err := applications.FindId(a.doc.DocID).One(&a.doc)
	if err == mgo.ErrNotFound {
		return errors.NotFoundf("application %q", a)
	}
	if err != nil {
		return errors.Errorf("cannot refresh application %q: %v", a, err)
	}
	return nil
}

// newUnitName returns the next unit name.
func (a *CAASApplication) newUnitName() (string, error) {
	unitSeq, err := sequence(a.st, a.Tag().String())
	if err != nil {
		return "", errors.Trace(err)
	}
	name := a.doc.Name + "/" + strconv.Itoa(unitSeq)
	return name, nil
}

// addCAASUnitOps returns a unique name for a new unit, and a list of txn operations
// necessary to create that unit. The asserts param can be used
// to include additional assertions for the application document.  This method
// assumes that the application already exists in the db.
func (a *CAASApplication) addCAASUnitOps(asserts bson.D) (string, []txn.Op, error) {
	var cons constraints.Value
	var storageCons map[string]StorageConstraints
	args := caasApplicationAddCAASUnitOpsArgs{
		cons:        cons,
		storageCons: storageCons,
	}

	names, ops, err := a.addCAASUnitOpsWithCons(args)
	if err != nil {
		return names, ops, err
	}
	// we verify the application is alive
	asserts = append(isAliveDoc, asserts...)
	ops = append(ops, a.incUnitCountOp(asserts))
	return names, ops, err
}

// the members of this type are currently ignored for CAASApplications, but remain for compatibility with State.
type caasApplicationAddCAASUnitOpsArgs struct {
	cons        constraints.Value
	storageCons map[string]StorageConstraints
}

func (a *CAASApplication) addApplicationUnitOps(args caasApplicationAddCAASUnitOpsArgs) (string, []txn.Op, error) {
	names, ops, err := a.addCAASUnitOpsWithCons(args)
	if err == nil {
		ops = append(ops, a.incUnitCountOp(nil))
	}
	return names, ops, err
}

func (a *CAASApplication) addCAASUnitOpsWithCons(args caasApplicationAddCAASUnitOpsArgs) (string, []txn.Op, error) {
	name, err := a.newUnitName()
	if err != nil {
		return "", nil, err
	}

	docID := a.st.docID(name)
	globalKey := unitGlobalKey(name)
	agentGlobalKey := unitAgentGlobalKey(name)
	udoc := &caasUnitDoc{
		DocID:           docID,
		Name:            name,
		CAASApplication: a.doc.Name,
		Life:            Alive,
	}
	now := a.st.clock.Now()
	agentStatusDoc := statusDoc{
		Status:  status.Allocating,
		Updated: now.UnixNano(),
	}
	unitStatusDoc := statusDoc{
		Status:     status.Waiting,
		StatusInfo: status.MessageWaitForMachine,
		Updated:    now.UnixNano(),
	}
	workloadVersionDoc := statusDoc{
		Status:  status.Unknown,
		Updated: now.UnixNano(),
	}

	ops, err := addCAASUnitOps(a.st, addCAASUnitOpsArgs{
		caasUnitDoc:        udoc,
		agentStatusDoc:     agentStatusDoc,
		workloadStatusDoc:  unitStatusDoc,
		workloadVersionDoc: workloadVersionDoc,
		meterStatusDoc:     &meterStatusDoc{Code: MeterNotSet.String()},
	})
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	// At the last moment we still have the statusDocs in scope, set the initial
	// history entries. This is risky, and may lead to extra entries, but that's
	// an intrinsic problem with mixing txn and non-txn ops -- we can't sync
	// them cleanly.
	probablyUpdateStatusHistory(a.st, globalKey, unitStatusDoc)
	probablyUpdateStatusHistory(a.st, agentGlobalKey, agentStatusDoc)
	probablyUpdateStatusHistory(a.st, globalWorkloadVersionKey(name), workloadVersionDoc)
	return name, ops, nil
}

// incUnitCountOp returns the operation to increment the application's unit count.
func (a *CAASApplication) incUnitCountOp(asserts bson.D) txn.Op {
	op := txn.Op{
		C:      caasApplicationsC,
		Id:     a.doc.DocID,
		Update: bson.D{{"$inc", bson.D{{"unitcount", 1}}}},
	}
	if len(asserts) > 0 {
		op.Assert = asserts
	}
	return op
}

// AddCAASUnit adds a new unit to the application.
func (a *CAASApplication) AddCAASUnit() (caasunit *CAASUnit, err error) {
	defer errors.DeferredAnnotatef(&err, "cannot add CAASUnit to CAASApplication %q", a)
	name, ops, err := a.addCAASUnitOps(nil)
	if err != nil {
		return nil, err
	}

	if err := a.st.runTransaction(ops); err == txn.ErrAborted {
		if alive, err := isAlive(a.st, caasApplicationsC, a.doc.DocID); err != nil {
			return nil, err
		} else if !alive {
			return nil, errors.New("application is not alive")
		}
		return nil, errors.New("inconsistent state")
	} else if err != nil {
		return nil, err
	}
	return a.st.CAASUnit(name)
}

// removeUnitOps returns the operations necessary to remove the supplied unit,
// assuming the supplied asserts apply to the unit document.
func (a *CAASApplication) removeUnitOps(u *CAASUnit, asserts bson.D) ([]txn.Op, error) {
	observedFieldsMatch := bson.D{
		{"charmurl", u.doc.CharmURL},
	}
	var ops []txn.Op
	ops = append(ops,
		txn.Op{
			C:      caasUnitsC,
			Id:     u.doc.DocID,
			Assert: append(observedFieldsMatch, asserts...),
			Remove: true,
		},
		removeMeterStatusOp(a.st, u.globalMeterStatusKey()),
		removeStatusOp(a.st, u.globalAgentKey()),
		removeStatusOp(a.st, u.globalKey()),
		removeConstraintsOp(u.globalAgentKey()),
		annotationRemoveOp(a.st, u.globalKey()),
		newCleanupOp(cleanupRemovedUnit, u.doc.Name),
	)

	if u.doc.CharmURL != nil {
		decOps, err := appCharmDecRefOps(a.st, a.doc.Name, u.doc.CharmURL)
		if errors.IsNotFound(err) {
			return nil, errRefresh
		} else if err != nil {
			return nil, err
		}
		ops = append(ops, decOps...)
	}
	if a.doc.Life == Dying && a.doc.RelationCount == 0 && a.doc.UnitCount == 1 {
		hasLastRef := bson.D{{"life", Dying}, {"relationcount", 0}, {"unitcount", 1}}
		removeOps, err := a.removeOps(hasLastRef)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return append(ops, removeOps...), nil
	}
	svcOp := txn.Op{
		C:      caasApplicationsC,
		Id:     a.doc.DocID,
		Update: bson.D{{"$inc", bson.D{{"unitcount", -1}}}},
	}
	if a.doc.Life == Alive {
		svcOp.Assert = bson.D{{"life", Alive}, {"unitcount", bson.D{{"$gt", 0}}}}
	} else {
		svcOp.Assert = bson.D{
			{"life", Dying},
			{"$or", []bson.D{
				{{"unitcount", bson.D{{"$gt", 1}}}},
				{{"relationcount", bson.D{{"$gt", 0}}}},
			}},
		}
	}
	ops = append(ops, svcOp)

	return ops, nil
}

func removeCAASUnitResourcesOps(st *State, unitID string) ([]txn.Op, error) {
	persist, err := st.ResourcesPersistence()
	if errors.IsNotSupported(err) {
		// Nothing to see here, move along.
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	ops, err := persist.NewRemoveUnitResourcesOps(unitID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ops, nil
}

// AllCAASUnits returns all units of the CAAS application.
func (a *CAASApplication) AllCAASUnits() (units []*CAASUnit, err error) {
	return allCAASUnits(a.st, a.doc.Name)
}

func allCAASUnits(st *CAASState, caasApplication string) (units []*CAASUnit, err error) {
	caasUnitsCollection, closer := st.database.GetCollection(caasUnitsC)
	defer closer()

	docs := []caasUnitDoc{}
	err = caasUnitsCollection.Find(bson.D{{"caasapplication", caasApplication}}).All(&docs)
	if err != nil {
		return nil, errors.Errorf("cannot get all caasunits from caasapplication %q: %v", caasApplication, err)
	}
	for i := range docs {
		units = append(units, newCAASUnit(st, &docs[i]))
	}
	return units, nil
}

func caasApplicationRelations(st *CAASState, name string) (relations []*Relation, err error) {
	defer errors.DeferredAnnotatef(&err, "can't get relations for application %q", name)
	relationsCollection, closer := st.database.GetCollection(relationsC)
	defer closer()

	docs := []relationDoc{}
	err = relationsCollection.Find(bson.D{{"endpoints.applicationname", name}}).All(&docs)
	if err != nil {
		return nil, err
	}
	// XXX
	// for _, v := range docs {
	// 	relations = append(relations, newRelation(st, &v))
	// }
	return relations, nil
}

// Relations returns a Relation for every relation the application is in.
func (a *CAASApplication) Relations() (relations []*Relation, err error) {
	return caasApplicationRelations(a.st, a.doc.Name)
}

// ConfigSettings returns the raw user configuration for the application's charm.
// Unset values are omitted.
func (a *CAASApplication) ConfigSettings() (charm.Settings, error) {
	settings, err := readSettings(a.st, settingsC, a.settingsKey())
	if err != nil {
		return nil, err
	}
	return settings.Map(), nil
}

// addCAASApplicationOps returns the operations required to add an application to the
// applications collection, along with all the associated expected other application
// entries. This method is used by both the *State.AddCAASApplication method and the
// migration import code.
func addCAASApplicationOps(st *CAASState, args addCAASApplicationOpsArgs) ([]txn.Op, error) {
	app := newCAASApplication(st, args.appDoc)

	charmRefOps, err := appCharmIncRefOps(st, args.appDoc.Name, args.appDoc.CharmURL, true)
	if err != nil {
		return nil, errors.Trace(err)
	}

	settingsKey := app.settingsKey()

	ops := []txn.Op{
		createSettingsOp(settingsC, settingsKey, args.settings),
		// XXX addModelCAASApplicationRefOp(st, app.Name()),
	}
	ops = append(ops, charmRefOps...)
	ops = append(ops, txn.Op{
		C:      caasApplicationsC,
		Id:     app.Name(),
		Assert: txn.DocMissing,
		Insert: args.appDoc,
	})
	return ops, nil
}

type addCAASApplicationOpsArgs struct {
	appDoc   *caasApplicationDoc
	settings map[string]interface{}
}
