// Copyright 2013, 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package caasprovisioner

import (
	"gopkg.in/juju/names.v2"

	"github.com/juju/juju/api/base"
	"github.com/juju/juju/api/common"
	apiwatcher "github.com/juju/juju/api/watcher"
	"github.com/juju/juju/apiserver/params"
	"github.com/juju/juju/network"
	"github.com/juju/juju/watcher"
)

// State provides access to the Machiner API facade.
type State struct {
	*common.ControllerConfigAPI

	facade base.FacadeCaller
}

// NewState creates a new client-side Machiner facade.
func NewState(caller base.APICaller) *State {
	facadeCaller := base.NewFacadeCaller(caller, "CAASProvisioner")
	return &State{
		ControllerConfigAPI: common.NewControllerConfig(facadeCaller),

		facade: facadeCaller,
	}
}

func (st *State) APIHostPorts() ([][]network.HostPort, error) {
	return nil, nil
}

func (st *State) ControllerTag() (names.ControllerTag, error) {
	return names.NewControllerTag(""), nil
}

func (st *State) ModelTag() (names.ModelTag, error) {
	return names.NewModelTag(""), nil
}

func (st *State) Endpoint() (string, error) {
	return "", nil
}

func (st *State) CAData() ([]byte, error) {
	return nil, nil
}

func (st *State) CertData() ([]byte, error) {
	return nil, nil
}

func (st *State) KeyData() ([]byte, error) {
	return nil, nil
}

// WatchApplications returns a StringsWatcher that notifies of
// changes to the lifecycles of applications in the current model.
func (st *State) WatchApplications() (watcher.StringsWatcher, error) {
	var result params.StringsWatchResult
	err := st.facade.FacadeCall("WatchApplications", nil, &result)
	if err != nil {
		return nil, err
	}
	if err := result.Error; err != nil {
		return nil, result.Error
	}
	w := apiwatcher.NewStringsWatcher(st.facade.RawAPICaller(), result)
	return w, nil
}
