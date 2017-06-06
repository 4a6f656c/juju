// Copyright 2014, 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// Package application provides access to the application api facade.
// This facade contains api calls that are specific to applications.
// As a rule of thumb, if the argument for an api requires an application name
// and affects only that application then the call belongs here.
package caasapplication

import (
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/juju/names.v2"

	"github.com/juju/juju/api/base"
	"github.com/juju/juju/apiserver/params"
	"github.com/juju/juju/charmstore"
)

var logger = loggo.GetLogger("juju.api.caasapplication")

// Client allows access to the service API end point.
type Client struct {
	base.ClientFacade
	st     base.APICallCloser
	facade base.FacadeCaller
}

// NewClient creates a new client for accessing the application api.
func NewClient(st base.APICallCloser) *Client {
	frontend, backend := base.NewClientFacade(st, "CAASApplication")
	return &Client{ClientFacade: frontend, st: st, facade: backend}
}

// ModelUUID returns the model UUID from the client connection.
func (c *Client) ModelUUID() string {
	tag, ok := c.st.ModelTag()
	if !ok {
		logger.Warningf("controller-only API connection has no model tag")
	}
	return tag.Id()
}

// DeployArgs holds the arguments to be sent to Client.ServiceDeploy.
type DeployArgs struct {

	// CharmID identifies the charm to deploy.
	CharmID charmstore.CharmID

	// ApplicationName is the name to give the application.
	ApplicationName string

	// NumUnits is the number of units to deploy.
	NumUnits int

	// ConfigYAML is a string that overrides the default config.yml.
	ConfigYAML string
}

// Deploy obtains the charm, either locally or from the charm store, and deploys
// it. Placement directives, if provided, specify the machine on which the charm
// is deployed.
func (c *Client) Deploy(args DeployArgs) error {
	deployArgs := params.CAASApplicationsDeploy{
		Applications: []params.CAASApplicationDeploy{{
			ApplicationName: args.ApplicationName,
			CharmURL:        args.CharmID.URL.String(),
			Channel:         string(args.CharmID.Channel),
			ConfigYAML:      args.ConfigYAML,
			NumUnits:        args.NumUnits,
		}},
	}
	var results params.ErrorResults
	var err error
	err = c.facade.FacadeCall("Deploy", deployArgs, &results)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(results.OneError())
}

// DestroyApplications destroys the given applications.
func (c *Client) DestroyApplications(appNames ...string) ([]params.DestroyApplicationResult, error) {
	args := params.Entities{
		Entities: make([]params.Entity, 0, len(appNames)),
	}
	allResults := make([]params.DestroyApplicationResult, len(appNames))
	index := make([]int, 0, len(appNames))
	for i, name := range appNames {
		if !names.IsValidApplication(name) {
			allResults[i].Error = &params.Error{
				Message: errors.NotValidf("application name %q", name).Error(),
			}
			continue
		}
		index = append(index, i)
		args.Entities = append(args.Entities, params.Entity{
			Tag: names.NewApplicationTag(name).String(),
		})
	}
	if len(args.Entities) > 0 {
		var result params.DestroyApplicationResults
		if err := c.facade.FacadeCall("DestroyApplication", args, &result); err != nil {
			return nil, errors.Trace(err)
		}
		if n := len(result.Results); n != len(args.Entities) {
			return nil, errors.Errorf("expected %d result(s), got %d", len(args.Entities), n)
		}
		for i, result := range result.Results {
			allResults[index[i]] = result
		}
	}
	return allResults, nil
}

func (c *Client) AddCAASUnits(appName string, numUnits int) ([]string, error) {
	args := params.AddApplicationUnits{
		ApplicationName: appName,
		NumUnits:        numUnits,
	}
	var result params.AddApplicationUnitsResults
	if err := c.facade.FacadeCall("AddUnits", args, &result); err != nil {
		return nil, errors.Trace(err)
	}
	return result.Units, nil
}

func (c *Client) AddCAASRelation(endpoints ...string) (*params.AddRelationResults, error) {
	var addRelRes params.AddRelationResults
	params := params.AddRelation{Endpoints: endpoints}
	err := c.facade.FacadeCall("AddRelation", params, &addRelRes)
	return &addRelRes, err
}

func (c *Client) DestroyCAASRelation(endpoints ...string) error {
	params := params.DestroyRelation{Endpoints: endpoints}
	return c.facade.FacadeCall("DestroyRelation", params, nil)
}

func (c *Client) Consume(remoteApplication, alias string) (string, error) {
	var consumeRes params.ConsumeApplicationResults
	args := params.ConsumeApplicationArgs{
		Args: []params.ConsumeApplicationArg{{
			ApplicationURL:   remoteApplication,
			ApplicationAlias: alias,
		}},
	}
	err := c.facade.FacadeCall("Consume", args, &consumeRes)
	if err != nil {
		return "", errors.Trace(err)
	}
	if resultLen := len(consumeRes.Results); resultLen != 1 {
		return "", errors.Errorf("expected 1 result, got %d", resultLen)
	}
	if err := consumeRes.Results[0].Error; err != nil {
		return "", errors.Trace(err)
	}
	return consumeRes.Results[0].LocalName, nil
}
