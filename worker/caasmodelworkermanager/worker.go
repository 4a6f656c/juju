// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package caasmodelworkermanager

import (
	"time"

	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/juju/names.v2"
	"gopkg.in/juju/worker.v1"

	"github.com/juju/juju/state"
	"github.com/juju/juju/worker/catacomb"
)

var logger = loggo.GetLogger("juju.workers.caasmodelworkermanager")

// Backend defines the State functionality used by the manager worker.
type Backend interface {
	WatchCAASModels() state.StringsWatcher
	ForCAASModel(modelTag names.ModelTag) (*state.CAASState, error)
}

type NewStateFunc func() (*state.CAASState, error)

// NewWorkerFunc should return a worker responsible for running
// all a model's required workers; and for returning nil when
// there's no more model to manage.
type NewWorkerFunc func(string, string) (worker.Worker, error)

// Config holds the dependencies and configuration necessary to run
// a model worker manager.
type Config struct {
	ControllerUUID string
	Backend        Backend
	NewWorker      NewWorkerFunc
	ErrorDelay     time.Duration
}

// Validate returns an error if config cannot be expected to drive
// a functional model worker manager.
func (config Config) Validate() error {
	if config.ControllerUUID == "" {
		return errors.NotValidf("missing controller UUID")
	}
	if config.Backend == nil {
		return errors.NotValidf("nil Backend")
	}
	if config.NewWorker == nil {
		return errors.NotValidf("nil NewWorker")
	}
	if config.ErrorDelay <= 0 {
		return errors.NotValidf("non-positive ErrorDelay")
	}
	return nil
}

func New(config Config) (worker.Worker, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	m := &caasModelWorkerManager{
		config: config,
	}

	err := catacomb.Invoke(catacomb.Plan{
		Site: &m.catacomb,
		Work: m.loop,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m, nil
}

type caasModelWorkerManager struct {
	catacomb catacomb.Catacomb
	config   Config
	runner   *worker.Runner
}

// Kill satisfies the Worker interface.
func (m *caasModelWorkerManager) Kill() {
	m.catacomb.Kill(nil)
}

// Wait satisfies the Worker interface.
func (m *caasModelWorkerManager) Wait() error {
	return m.catacomb.Wait()
}

func (m *caasModelWorkerManager) loop() error {
	m.runner = worker.NewRunner(worker.RunnerParams{
		IsFatal:       neverFatal,
		MoreImportant: neverImportant,
		RestartDelay:  m.config.ErrorDelay,
	})
	if err := m.catacomb.Add(m.runner); err != nil {
		return errors.Trace(err)
	}
	watcher := m.config.Backend.WatchCAASModels()
	if err := m.catacomb.Add(watcher); err != nil {
		return errors.Trace(err)
	}

	for {
		select {
		case <-m.catacomb.Dying():
			return m.catacomb.ErrDying()
		case uuids, ok := <-watcher.Changes():
			if !ok {
				return errors.New("changes stopped")
			}
			for _, modelUUID := range uuids {
				if err := m.ensure(modelUUID); err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

func (m *caasModelWorkerManager) ensure(modelUUID string) error {
	starter := m.starter(modelUUID)
	if err := m.runner.StartWorker(modelUUID, starter); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *caasModelWorkerManager) starter(modelUUID string) func() (worker.Worker, error) {
	return func() (worker.Worker, error) {
		logger.Debugf("starting workers for CAAS model %q", modelUUID)

		worker, err := m.config.NewWorker(m.config.ControllerUUID, modelUUID)
		if err != nil {
			return nil, errors.Annotatef(err, "cannot manage CAAS model %q", modelUUID)
		}
		return worker, nil
	}
}

func neverFatal(error) bool {
	return false
}

func neverImportant(error, error) bool {
	return false
}
