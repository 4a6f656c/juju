// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package caasprovisioner

import (
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/juju/names.v2"
	"gopkg.in/juju/worker.v1"

	"github.com/juju/juju/agent"
	"github.com/juju/juju/network"
	"github.com/juju/juju/state"
	"github.com/juju/juju/version"
	"github.com/juju/juju/worker/caasmodelworkermanager"
	"github.com/juju/juju/worker/catacomb"
)

var logger = loggo.GetLogger("juju.workers.caasprovisioner")

func New(newState caasmodelworkermanager.NewStateFunc) (worker.Worker, error) {
	p := &provisioner{
		newState: newState,
	}
	err := catacomb.Invoke(catacomb.Plan{
		Site: &p.catacomb,
		Work: p.loop,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p, nil
}

type provisioner struct {
	catacomb catacomb.Catacomb
	newState caasmodelworkermanager.NewStateFunc
}

// Kill is part of the worker.Worker interface.
func (p *provisioner) Kill() {
	p.catacomb.Kill(nil)
}

// Wait is part of the worker.Worker interface.
func (p *provisioner) Wait() error {
	return p.catacomb.Wait()
}

func (p *provisioner) loop() error {
	st, err := p.newState()
	if err != nil {
		return errors.Annotate(err, "opening state")
	}
	defer st.Close()

	// XXX this assumes the k8s credentials never change. This is fine
	// for the prototype but needs to be considered for any real
	// implementation.
	client, err := newK8sClient(st)
	if err != nil {
		return errors.Annotate(err, "creating k8s client")
	}

	newConfig := func(appName string) ([]byte, error) {
		return newOperatorConfig(appName, st)
	}

	// XXX this loop should also keep an eye on kubernetes and ensure
	// that the operator stays up, redeploying it if the pod goes
	// away. For some runtimes we *could* rely on the the runtime's
	// features to do this.
	w := st.WatchApplications()
	p.catacomb.Add(w)
	for {
		select {
		case apps := <-w.Changes():
			for _, app := range apps {
				logger.Infof("saw app: %s", app)
				if err := ensureOperator(client, app, newConfig); err != nil {
					// XXX need retry logic rather than just giving up
					// (see queue concept in storage provisioner)
					logger.Errorf("ensure failed: %v", err)
					return errors.Trace(err)
				}
			}
		case <-p.catacomb.Dying():
			return p.catacomb.ErrDying()
		}
	}
}

func newOperatorConfig(appName string, st *state.CAASState) ([]byte, error) {
	appTag := names.NewApplicationTag(appName)

	apiAddrs, err := apiAddresses(st)
	if err != nil {
		return nil, errors.Trace(err)
	}

	controllerCfg, err := st.ControllerConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	caCert, ok := controllerCfg.CACert()
	if !ok {
		return nil, errors.New("missing ca cert in controller config")
	}

	conf, err := agent.NewAgentConfig(
		agent.AgentConfigParams{
			Paths: agent.Paths{
				// XXX shouldn't be hardcoded
				DataDir: "/var/lib/juju",
				LogDir:  "/var/log/juju",
			},
			// This isn't actually used but needs to be supplied.
			UpgradedToVersion: version.Current,
			Tag:               appTag,
			Password:          "XXX not currently checked",
			Controller:        st.ControllerTag(),
			Model:             st.ModelTag(),
			APIAddresses:      apiAddrs,
			CACert:            caCert,
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	confBytes, err := conf.Render()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return confBytes, nil
}

func apiAddresses(st *state.CAASState) ([]string, error) {
	apiHostPorts, err := st.APIHostPorts()
	if err != nil {
		return nil, err
	}
	var addrs = make([]string, 0, len(apiHostPorts))
	for _, hostPorts := range apiHostPorts {
		ordered := network.PrioritizeInternalHostPorts(hostPorts, false)
		for _, addr := range ordered {
			if addr != "" {
				addrs = append(addrs, addr)
			}
		}
	}
	return addrs, nil
}
