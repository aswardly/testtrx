//Package datamapper provides the definitions of datamapper
package datamapper

import (
	"fmt"
	"testtrx/model"

	"github.com/go-errors/errors"
	"github.com/gocql/gocql"
)

//User is a struct of datamapper for user domain model
type User struct {
	dbSession     *gocql.Session //database connection session object
	pagedQuery    *gocql.Query   //query object (storing this is required for result paging)
	pageSize      int            //size of page (no of records per page) for query result paging
	nextPageState []byte         //page state of next page for result paging purpose
}

//NewUser is a function for initializing a new user datamapper
func NewUser(session *gocql.Session) *User {
	//Note: pageSize defaults to 10
	return &User{session, nil, 10, nil}
}

//SetPageSize is a function for setting query result page size (no of records perpage)
func (u *User) SetPageSize(size int) {
	u.pageSize = size
}

//FindByID is a function for finding an user by id
func (u *User) FindByID(id string) (*model.User, *errors.Error) {
	userModel := model.User{}

	if err := u.dbSession.Query(`SELECT 
			user_email,
			password,
			name,
			status,
			last_activity,
			auth_token,
			google_token,
			facebook_token
			FROM user
			WHERE user_email = ? LIMIT 1`, id).
		Consistency(gocql.One).
		Scan(&userModel.Email,
			&userModel.Password,
			&userModel.Name,
			&userModel.Status,
			&userModel.LastActivity,
			&userModel.AuthToken,
			&userModel.GoogleToken,
			&userModel.FacebookToken); err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return &userModel, nil
}

//FindAll is a function for finding all user with paging capability
func (u *User) FindAll() ([]*model.User, *errors.Error) {
	u.pagedQuery = u.dbSession.Query(`SELECT
		user_email,
		password,
		name,
		status,
		last_activity,
		auth_token,
		google_token,
		facebook_token
	FROM user`)
	iter := u.pagedQuery.PageState(nil).PageSize(u.pageSize).Iter()
	//the iterator page state becomes page state for next page
	u.nextPageState = iter.PageState()

	return u.scanQueryResult(iter)
}

//NextPage is a function for getting the previous page query results of the previously executed 'select' query
func (u *User) NextPage() ([]*model.User, *errors.Error, bool) {
	if nil == u.pagedQuery {
		return nil,  errors.Wrap(fmt.Errorf("Can't iterate next page, no query has been performed"), 0), true
	}
	temp := u.nextPageState
	iter := u.pagedQuery.PageState(temp).PageSize(u.pageSize).Iter()

	//check whether iter.PageState() is empty, if it is empty then we have reached the last page
	if len(iter.PageState()) == 0 {
		//reset properties
		u.nextPageState = nil
		u.pagedQuery = nil

		modelSlice, err := u.scanQueryResult(iter)
		return modelSlice, err, true
	}
	//else we still have more pages to iterate
	//the iterator page state becomes page state for the next page
	u.nextPageState = iter.PageState()
	modelSlice, err := u.scanQueryResult(iter)
	return modelSlice, err, false 
}

//scanQueryResult is a function for scanning records to model objects from an iterator of query result
func (u *User) scanQueryResult(iter *gocql.Iter) ([]*model.User, *errors.Error) {
	var done = false
	var userList []*model.User

	for done == false {
		userModel := model.User{}
		ok := iter.Scan(
			&userModel.Email,
			&userModel.Password,
			&userModel.Name,
			&userModel.Status,
			&userModel.LastActivity,
			&userModel.AuthToken,
			&userModel.GoogleToken,
			&userModel.FacebookToken,
		)
		if ok {
			userList = append(userList, &userModel)
		} else {
			done = true
		}
	}
	//close the iterator (to get any errors that occured during or after iteration)
	//see: https://github.com/gocql/gocql/issues/57#issuecomment-25573670
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return userList, nil
}

//Insert is a function for inserting new user
func (u *User) Insert(user *model.User) (bool, *errors.Error) {

	if err := u.dbSession.Query(`
		INSERT INTO user (
			user_email, 
			password, 
			name,
			status, 
			last_activity,
			auth_token,
			google_token,
			facebook_token
			 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		user.Email,
		user.Password,
		user.Name,
		user.Status,
		//Note: for consistency when saving and loading time data into/from cassandra,
		//always convert timezone to UTC prior to saving time in gocql
		//This is because cassandra only stores time as unix timestamp (no timezone info)
		//and gocql always assumed the timezone to be UTC when loading timestamp data
		user.LastActivity.UTC(),
		user.AuthToken,
		user.GoogleToken,
		user.FacebookToken,
	).Exec(); err != nil {
		return false, errors.Wrap(err, 0)
	}
	return true, nil
}

//Update is a function for updating a user
func (u *User) Update(user *model.User) (bool, *errors.Error) {

	//Note: user_email and name cannot be updated since they are part of the primary key (fields part of primary key can't be updated in cassandra)
	if err := u.dbSession.Query(`
		UPDATE user SET
			password = ?,
			status = ?,
			last_activity = ?,
			auth_token = ?,
			google_token = ?,
			facebook_token = ?
		WHERE user_email = ? AND name = ? IF EXISTS`,
		user.Password,
		user.Status,
		//Note: for consistency when saving and loading time data into/from cassandra,
		//always convert timezone to UTC prior to saving time in gocql
		//This is because cassandra only stores time as unix timestamp (no timezone info)
		//and gocql always assumed the timezone to be UTC when loading timestamp data
		user.LastActivity.UTC(),
		user.AuthToken,
		user.GoogleToken,
		user.FacebookToken,
		user.Email,
		user.Name).Exec(); err != nil {
		return false, errors.Wrap(err, 0)
	}
	//NOTE: there is no way to get affected rows of an update/delete query in cassandra
	//see: https://stackoverflow.com/questions/28611459/how-to-know-affected-rows-in-cassandracql
	//if the query was performed successfully without any problem, then consider query is successful
	return true, nil
}

//Delete is a function for deleting user
func (u *User) Delete(user *model.User) (bool, *errors.Error) {
	if err := u.dbSession.Query(`
		DELETE FROM user 
		WHERE user_email = ? AND name = ? IF EXISTS`,
		user.Email,
		user.Name).Exec(); err != nil {
		return false, errors.Wrap(err, 0)
	}
	//NOTE: there is no way to get affected rows of an update/delete query in cassandra
	//see: https://stackoverflow.com/questions/28611459/how-to-know-affected-rows-in-cassandracql
	//if the query was performed successfully without any problem, then consider query is successful
	return true, nil
}
