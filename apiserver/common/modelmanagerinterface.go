// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package common

import (
	"time"

	"github.com/juju/description"
	"gopkg.in/juju/names.v2"

	"github.com/juju/juju/controller"
	"github.com/juju/juju/environs"
	"github.com/juju/juju/environs/config"
	"github.com/juju/juju/permission"
	"github.com/juju/juju/state"
	"github.com/juju/juju/status"
)

// ModelManagerBackend defines methods provided by a state
// instance used by the model manager apiserver implementation.
// All the interface methods are defined directly on state.State
// and are reproduced here for use in tests.
type ModelManagerBackend interface {
	APIHostPortsGetter
	ToolsStorageGetter
	BlockGetter
	state.CloudAccessor

	ModelUUID() string
	ModelsForUser(names.UserTag) ([]*state.UserModel, error)
	IsControllerAdmin(user names.UserTag) (bool, error)
	NewModel(state.ModelArgs) (Model, ModelManagerBackend, error)
	NewCAASModel(state.CAASModelArgs) (CAASModel, CAASModelBackend, error)

	ComposeNewModelConfig(modelAttr map[string]interface{}, regionSpec *environs.RegionSpec) (map[string]interface{}, error)
	ControllerModel() (Model, error)
	ControllerConfig() (controller.Config, error)
	ForCAASModel(tag names.ModelTag) (CAASModelBackend, error)
	ForModel(tag names.ModelTag) (ModelManagerBackend, error)
	GetModel(names.ModelTag) (Model, error)
	IsCAASModel(uuid string) (bool, error)
	Model() (Model, error)
	ModelConfigDefaultValues() (config.ModelDefaultAttributes, error)
	UpdateModelConfigDefaultValues(update map[string]interface{}, remove []string, regionSpec *environs.RegionSpec) error
	Unit(name string) (*state.Unit, error)
	ModelTag() names.ModelTag
	ModelConfig() (*config.Config, error)
	AllModels() ([]Model, error)
	AddModelUser(string, state.UserAccessSpec) (permission.UserAccess, error)
	AddControllerUser(state.UserAccessSpec) (permission.UserAccess, error)
	RemoveUserAccess(names.UserTag, names.Tag) error
	UserAccess(names.UserTag, names.Tag) (permission.UserAccess, error)
	AllMachines() (machines []Machine, err error)
	AllApplications() (applications []Application, err error)
	ControllerUUID() string
	ControllerTag() names.ControllerTag
	Export() (description.Model, error)
	SetUserAccess(subject names.UserTag, target names.Tag, access permission.Access) (permission.UserAccess, error)
	SetModelMeterStatus(string, string) error
	LastModelConnection(user names.UserTag) (time.Time, error)
	LatestMigration() (state.ModelMigration, error)
	DumpAll() (map[string]interface{}, error)
	Close() error

	// Methods required by the metricsender package.
	MetricsManager() (*state.MetricsManager, error)
	MetricsToSend(batchSize int) ([]*state.MetricBatch, error)
	SetMetricBatchesSent(batchUUIDs []string) error
	CountOfUnsentMetrics() (int, error)
	CountOfSentMetrics() (int, error)
	CleanupOldMetrics() error
}

// XXX
type CAASModelBackend interface {
	CAASModel() (CAASModel, error)
	ControllerUUID() string
	ModelUUID() string
	Close() error
}

// Model defines methods provided by a state.Model instance.
// All the interface methods are defined directly on state.Model
// and are reproduced here for use in tests.
type Model interface {
	Config() (*config.Config, error)
	Life() state.Life
	ModelTag() names.ModelTag
	Owner() names.UserTag
	Status() (status.StatusInfo, error)
	Cloud() string
	CloudCredential() (names.CloudCredentialTag, bool)
	CloudRegion() string
	Users() ([]permission.UserAccess, error)
	Destroy() error
	DestroyIncludingHosted() error
}

type CAASModel interface {
	Name() string
	ModelTag() names.ModelTag
	Owner() names.UserTag
}

var _ ModelManagerBackend = (*modelManagerStateShim)(nil)

type modelManagerStateShim struct {
	*state.State
}

var _ CAASModelBackend = (*caasModelBackendShim)(nil)

type caasModelBackendShim struct {
	*state.CAASState
}

// NewModelManagerBackend returns a modelManagerStateShim wrapping the passed
// state, which implements ModelManagerBackend.
func NewModelManagerBackend(st *state.State) ModelManagerBackend {
	return modelManagerStateShim{st}
}

// ControllerModel implements ModelManagerBackend.
func (st modelManagerStateShim) ControllerModel() (Model, error) {
	m, err := st.State.ControllerModel()
	if err != nil {
		return nil, err
	}
	return modelShim{m}, nil
}

// NewModel implements ModelManagerBackend.
func (st modelManagerStateShim) NewModel(args state.ModelArgs) (Model, ModelManagerBackend, error) {
	m, otherState, err := st.State.NewModel(args)
	if err != nil {
		return nil, nil, err
	}
	return modelShim{m}, modelManagerStateShim{otherState}, nil
}

// NewCAASModel implements ModelManagerBackend.
func (st modelManagerStateShim) NewCAASModel(args state.CAASModelArgs) (CAASModel, CAASModelBackend, error) {
	m, otherState, err := st.State.NewCAASModel(args)
	if err != nil {
		return nil, nil, err
	}
	return caasModelShim{m}, caasModelBackendShim{otherState}, nil
}

// ForCAASModel implements ModelManagerBackend.
func (st modelManagerStateShim) ForCAASModel(tag names.ModelTag) (CAASModelBackend, error) {
	otherState, err := st.State.ForCAASModel(tag)
	if err != nil {
		return nil, err
	}
	return caasModelBackendShim{otherState}, nil
}

// ForModel implements ModelManagerBackend.
func (st modelManagerStateShim) ForModel(tag names.ModelTag) (ModelManagerBackend, error) {
	otherState, err := st.State.ForModel(tag)
	if err != nil {
		return nil, err
	}
	return modelManagerStateShim{otherState}, nil
}

// GetModel implements ModelManagerBackend.
func (st modelManagerStateShim) GetModel(tag names.ModelTag) (Model, error) {
	m, err := st.State.GetModel(tag)
	if err != nil {
		return nil, err
	}
	return modelShim{m}, nil
}

// Model implements ModelManagerBackend.
func (st modelManagerStateShim) Model() (Model, error) {
	m, err := st.State.Model()
	if err != nil {
		return nil, err
	}
	return modelShim{m}, nil
}

// AllModels implements ModelManagerBackend.
func (st modelManagerStateShim) AllModels() ([]Model, error) {
	allStateModels, err := st.State.AllModels()
	if err != nil {
		return nil, err
	}
	all := make([]Model, len(allStateModels))
	for i, m := range allStateModels {
		all[i] = modelShim{m}
	}
	return all, nil
}

type modelShim struct {
	*state.Model
}

// Users implements ModelManagerBackend.
func (m modelShim) Users() ([]permission.UserAccess, error) {
	stateUsers, err := m.Model.Users()
	if err != nil {
		return nil, err
	}
	users := make([]permission.UserAccess, len(stateUsers))
	for i, user := range stateUsers {
		users[i] = user
	}
	return users, nil
}

type machineShim struct {
	*state.Machine
}

func (st modelManagerStateShim) AllMachines() ([]Machine, error) {
	allStateMachines, err := st.State.AllMachines()
	if err != nil {
		return nil, err
	}
	all := make([]Machine, len(allStateMachines))
	for i, m := range allStateMachines {
		all[i] = machineShim{m}
	}
	return all, nil
}

// Application defines methods provided by a state.Application instance.
type Application interface{}

type applicationShim struct {
	*state.Application
}

func (st modelManagerStateShim) AllApplications() ([]Application, error) {
	allStateApplications, err := st.State.AllApplications()
	if err != nil {
		return nil, err
	}
	all := make([]Application, len(allStateApplications))
	for i, a := range allStateApplications {
		all[i] = applicationShim{a}
	}
	return all, nil
}

type caasModelShim struct {
	*state.CAASModel
}

// CAASModel implements CAASModelBackend.
func (st caasModelBackendShim) CAASModel() (CAASModel, error) {
	m, err := st.CAASState.CAASModel()
	if err != nil {
		return nil, err
	}
	return caasModelShim{m}, nil
}

