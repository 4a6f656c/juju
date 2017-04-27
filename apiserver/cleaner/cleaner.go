// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// The cleaner package implements the API interface
// used by the cleaner worker.

package cleaner

import (
	"fmt"
	"os"

	"github.com/juju/juju/apiserver/common"
	"github.com/juju/juju/apiserver/facade"
	"github.com/juju/juju/apiserver/params"
	"github.com/juju/juju/state"
	"github.com/juju/juju/state/watcher"
)

type backend interface {
	Cleanup() error
	WatchCleanups() state.NotifyWatcher
}

// API implements the API used by the cleaner worker.
type API struct {
	st        StateInterface
	resources facade.Resources
}

// NewFacade creates a new instance of the Cleaner API.
func NewFacade(ctx facade.Context) (*API, error) {
	var st backend
	if ctx.IsCAAS() {
		st = ctx.CAASState()
	} else {
		st = ctx.State()
	}

	auth := ctx.Auth()
	resources := ctx.Resources()

	if !auth.AuthController() {
fmt.Fprintf(os.Stderr, "cleaner permission denied...\n")
//		return nil, common.ErrPerm
	}
	return &API{
		st:        st,
		resources: resources,
	}, nil
}

// Cleanup triggers a state cleanup
func (api *API) Cleanup() error {
	return api.st.Cleanup()
}

// WatchChanges watches for cleanups to be perfomed in state
func (api *API) WatchCleanups() (params.NotifyWatchResult, error) {
	watch := api.st.WatchCleanups()
	if _, ok := <-watch.Changes(); ok {
		return params.NotifyWatchResult{
			NotifyWatcherId: api.resources.Register(watch),
		}, nil
	}
	return params.NotifyWatchResult{
		Error: common.ServerError(watcher.EnsureErr(watch)),
	}, nil
}
