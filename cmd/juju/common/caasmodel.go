// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for infos.

package common

import (
	"time"

	"github.com/juju/errors"
	"gopkg.in/juju/names.v2"

	"github.com/juju/juju/apiserver/params"
)

// CAASModelInfo contains information about a CAAS model.
type CAASModelInfo struct {
	Name           string                      `json:"name" yaml:"name"`
	UUID           string                      `json:"model-uuid" yaml:"model-uuid"`
	ControllerUUID string                      `json:"controller-uuid" yaml:"controller-uuid"`
	ControllerName string                      `json:"controller-name" yaml:"controller-name"`
	Owner          string                      `json:"owner" yaml:"owner"`
}

// CAASModelInfoFromParams translates a params.CAASModelInfo to CAASModelInfo.
func CAASModelInfoFromParams(info params.CAASModelInfo, now time.Time) (CAASModelInfo, error) {
	tag, err := names.ParseUserTag(info.OwnerTag)
	if err != nil {
		return CAASModelInfo{}, errors.Trace(err)
	}
	return CAASModelInfo{
		Name:           info.Name,
		UUID:           info.UUID,
		ControllerUUID: info.ControllerUUID,
		Owner:          tag.Id(),
	}, nil
}
