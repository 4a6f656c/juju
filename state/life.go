package state

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/txn"
)

// Life represents the lifecycle state of the entities
// Relation, Unit, Service and Machine.
type Life int8

const (
	Alive Life = iota
	Dying
	Dead
	nLife
)

var notDead = D{{"life", D{{"$ne", Dead}}}}
var isAlive = D{{"life", Alive}}

var lifeStrings = [nLife]string{
	Alive: "alive",
	Dying: "dying",
	Dead:  "dead",
}

func (l Life) String() string {
	return lifeStrings[l]
}

// Living describes state entities with a lifecycle.
type Living interface {
	Life() Life
	EnsureDying() error
	EnsureDead() error
	Refresh() error
}

// ensureDying advances the specified entity's life status to Dying, if necessary.
func ensureDying(st *State, coll *mgo.Collection, id interface{}, desc string) error {
	ops := []txn.Op{{
		C:      coll.Name,
		Id:     id,
		Assert: isAlive,
		Update: D{{"$set", D{{"life", Dying}}}},
	}}
	if err := st.runner.Run(ops, "", nil); err == txn.ErrAborted {
		return nil
	} else if err != nil {
		return fmt.Errorf("cannot start termination of %s %#v: %v", desc, id, err)
	}
	return nil
}

// cannotKillError is returned from ensureDead when the targeted entity's
// lifecycle has failed to advance to (or beyond) Dead, due to assertion
// failures.
type cannotKillError struct {
	prefix, msg string
}

func (e *cannotKillError) Error() string {
	return fmt.Sprintf("%s: %s", e.prefix, e.msg)
}

// ensureDead advances the specified entity's life status to Dead, if necessary.
// Preconditions can be supplied in assertOps; if the preconditions fail, the error
// will contain assertMsg. If the entity is not found, no error is returned.
func ensureDead(st *State, coll *mgo.Collection, id interface{}, desc string, assertOps []txn.Op, assertMsg string) (err error) {
	errPrefix := fmt.Sprintf("cannot finish termination of %s %#v", desc, id)
	decorate := func(err error) error {
		return fmt.Errorf("%s: %v", errPrefix, err)
	}
	ops := append(assertOps, txn.Op{
		C:      coll.Name,
		Id:     id,
		Update: D{{"$set", D{{"life", Dead}}}},
	})
	if err = st.runner.Run(ops, "", nil); err == nil {
		return nil
	} else if err != txn.ErrAborted {
		return decorate(err)
	}
	var doc struct{ Life }
	if err = coll.FindId(id).One(&doc); err == mgo.ErrNotFound {
		return nil
	} else if err != nil {
		return decorate(err)
	} else if doc.Life != Dead {
		return &cannotKillError{errPrefix, assertMsg}
	}
	return nil
}
