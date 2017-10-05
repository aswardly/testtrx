//Package model provides the business domain models definitions
package model

import (
	"time"
)

//UserStatusActive is const for 'active' user status
const UserStatusActive string = "A"

//UserStatusInactive is const for 'inactive' user status
const UserStatusInactive string = "I"

//UserStatusDeleted is const for 'deleted' user status
const UserStatusDeleted string = "D"

//UserStatusMap is a map of known status code and its label pairs
var UserStatusMap = map[string]string{
	UserStatusActive:   "Active",
	UserStatusInactive: "Inactive",
	UserStatusDeleted:  "Deleted",
}

//User is business domain model definition of user
type User struct {
	Email         string
	Password      string
	Name          string
	Status        string
	LastActivity  time.Time
	AuthToken     string
	GoogleToken   string
	FacebookToken string
}

//GetID is a function for returning a user model id
func (u *User) GetID() string {
	return u.Email
}
