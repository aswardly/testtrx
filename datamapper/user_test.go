//user_test provides unit tests for user datamapper
package datamapper_test

import (
	"testtrx/datamapper"
	"testtrx/model"

	"github.com/go-errors/errors"
	"github.com/gocql/gocql"

	"os"
	"strconv"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	//run tests
	exitCode := m.Run()
	//test teardown
	teardownTest()
	os.Exit(exitCode)
}

func initUserMapperTest(tb testing.TB) *datamapper.User {
	session := initTest()
	return datamapper.NewUser(session)
}

func initUserTable(tb testing.TB) {
	session := initTest()

	err := session.Query(`DROP TABLE IF EXISTS user`).Exec()
	if err != nil {
		tb.Fatalf("Failed to drop table: %v", err)
	}

	err = session.Query(`CREATE TABLE user (
		user_email varchar,
		password varchar,
		name varchar,
		status varchar,
		last_activity timestamp,
		auth_token varchar,
		google_token varchar,
		facebook_token varchar,
	PRIMARY KEY ((user_email), name)
	) WITH CLUSTERING ORDER BY (name asc)`).Exec()

	if err != nil {
		tb.Fatalf("Failed to create table: %v", err)
	}
}

func cleanupUserTable(tb testing.TB) {
	session := initTest()

	err := session.Query(`DROP TABLE IF EXISTS user`).Exec()
	if err != nil {
		tb.Fatalf("Failed to drop table: %v", err)
	}
}

func TestInsert(t *testing.T) {
	session := initTest()
	initUserTable(t)
	userMapper := initUserMapperTest(t)

	var nowTime = time.Now()
	var userModelSlice []model.User

	//initiate the model objects to insert
	for i := 1; i <= 5; i++ {
		userModelSlice = append(userModelSlice, model.User{
			"user" + strconv.Itoa(i) + "@testEmail.com",
			"dummyPasswordHash",
			strconv.Itoa(i),
			model.UserStatusActive,
			nowTime,
			"dummyAuthToken" + strconv.Itoa(i),
			"dummyGoogleToken" + strconv.Itoa(i),
			"dummyFacebookToken" + strconv.Itoa(i)})
	}

	//insert the models
	for _, val := range userModelSlice {
		_, err := userMapper.Insert(&val)
		if err != nil {
			t.Errorf("Failed to insert user: %v", err)
		}
	}

	//confirm the inserted models by performing select query
	var userEmail, password, name, status, authToken, googleToken, facebookToken string
	var lastActivity time.Time

	// list all records
	//Note: we want to select all records without where condition with order by but can't do that in cassandra
	//see: https://stackoverflow.com/questions/39039672/why-cant-i-sort-by-the-primary-key-in-cql
	iter := session.Query(`SELECT 
		user_email,
		password,
		name,
		status,
		last_activity,
		auth_token,
		google_token,
		facebook_token
		 FROM user`).Iter()
	for iter.Scan(&userEmail, &password, &name, &status, &lastActivity, &authToken, &googleToken, &facebookToken) {
		counter, err := strconv.Atoi(name)
		if err != nil {
			t.Errorf("failed converting name: %v to integer counter", counter)
		}

		if "user"+strconv.Itoa(counter)+"@testEmail.com" != userEmail {
			t.Errorf("counter:%v, want %v for userEmail, got %v", counter, "user"+strconv.Itoa(counter)+"@testEmail.com", userEmail)
		}
		if "dummyPasswordHash" != password {
			t.Errorf("counter:%v, want %v for password, got %v", counter, "dummyPasswordHash", password)
		}
		if strconv.Itoa(counter) != name {
			t.Errorf("counter:%v, want %v for name, got %v", counter, "user"+strconv.Itoa(counter), name)
		}
		if model.UserStatusActive != status {
			t.Errorf("counter:%v, want %v for status, got %v", counter, model.UserStatusActive, status)
		}
		//Note on comparing time loaded from cassandra via gocql and time.Time instance created in code:
		//time.Time created in code has accuracy up to nanoseconds while time.Time loaded from gocql only has accuracy up to seconds
		//this can result in both time being unequal when compared, therefore we convert the time.TIme to unix timestamp prior to comparing
		if nowTime.Unix() != lastActivity.Unix() {
			t.Errorf("counter:%v, want %v for lastActivity, got %v", counter, nowTime.String(), lastActivity.String())
		}
		if "dummyAuthToken"+strconv.Itoa(counter) != authToken {
			t.Errorf("counter:%v, want %v for authToken, got %v", counter, "dummyAuthToken"+strconv.Itoa(counter), authToken)
		}
		if "dummyGoogleToken"+strconv.Itoa(counter) != googleToken {
			t.Errorf("counter:%v, want %v for googleToken, got %v", counter, "dummyGoogleToken"+strconv.Itoa(counter), googleToken)
		}
		if "dummyFacebookToken"+strconv.Itoa(counter) != facebookToken {
			t.Errorf("counter:%v, want %v for facebookToken, got %v", counter, "dummyFacebookToken"+strconv.Itoa(counter), facebookToken)
		}
	}
	if err := iter.Close(); err != nil {
		t.Errorf("Select query got error: %v", err)
	}
	cleanupUserTable(t)
}

func TestUpdate(t *testing.T) {
	session := initTest()
	initUserTable(t)
	userMapper := initUserMapperTest(t)

	var nowTime = time.Now()

	//initiate the model object to insert
	userModel := model.User{
		"user1@testEmail.com",
		"dummyPasswordHash",
		"user1",
		model.UserStatusActive,
		nowTime,
		"dummyAuthToken1",
		"dummyGoogleToken1",
		"dummyFacebookToken1"}

	//insert the models
	_, err := userMapper.Insert(&userModel)
	if err != nil {
		t.Errorf("Failed to insert user: %v", err)
	}

	//update the user
	userModel.Status = model.UserStatusInactive
	userModel.AuthToken = "updatedDummyAuthToken"

	_, err = userMapper.Update(&userModel)
	if err != nil {
		t.Errorf("Failed to update user: %v", err)
	}

	//check whether user has really been updated
	var userEmail, password, name, status, authToken, googleToken, facebookToken string
	var lastActivity time.Time

	if err := session.Query(`SELECT 
		user_email,
		password,
		name,
		status,
		last_activity,
		auth_token,
		google_token,
		facebook_token
		 FROM user WHERE user_email = 'user1@testEmail.com'`).
		Consistency(gocql.One).
		Scan(&userEmail,
			&password,
			&name,
			&status,
			&lastActivity,
			&authToken,
			&googleToken,
			&facebookToken); err != nil {
		t.Errorf("Failed to perform select query: %v", err)
	}

	if "user1@testEmail.com" != userEmail {
		t.Errorf("want %v for userEmail, got %v", "user1@testEmail.com", userEmail)
	}
	if "dummyPasswordHash" != password {
		t.Errorf("want %v for password, got %v", "dummyPasswordHash", password)
	}
	if "user1" != name {
		t.Errorf("want %v for name, got %v", "user1", name)
	}
	if model.UserStatusInactive != status {
		t.Errorf("want %v for status, got %v", model.UserStatusInactive, status)
	}
	//Note on comparing time loaded from cassandra via gocql and time.Time instance created in code:
	//time.Time created in code has accuracy up to nanoseconds while time.Time loaded from gocql only has accuracy up to seconds
	//this can result in both time being unequal when compared, therefore we convert the time.TIme to unix timestamp prior to comparing
	if nowTime.Unix() != lastActivity.Unix() {
		t.Errorf("want %v for lastActivity, got %v", nowTime.String(), lastActivity.String())
	}
	if "updatedDummyAuthToken" != authToken {
		t.Errorf("want %v for authToken, got %v", "updatedDummyAuthToken", authToken)
	}
	if "dummyGoogleToken1" != googleToken {
		t.Errorf("want %v for googleToken, got %v", "dummyGoogleToken1", googleToken)
	}
	if "dummyFacebookToken1" != facebookToken {
		t.Errorf("want %v for facebookToken, got %v", "dummyFacebookToken1", facebookToken)
	}
	cleanupUserTable(t)
}

func TestDelete(t *testing.T) {
	session := initTest()
	initUserTable(t)
	userMapper := initUserMapperTest(t)

	var nowTime = time.Now()

	//initiate the model object to insert
	userModel := model.User{
		"user1@testEmail.com",
		"dummyPasswordHash",
		"user1",
		model.UserStatusActive,
		nowTime,
		"dummyAuthToken1",
		"dummyGoogleToken1",
		"dummyFacebookToken1"}

	//insert the models
	_, err := userMapper.Insert(&userModel)
	if err != nil {
		t.Errorf("Failed to insert user: %v", err)
	}

	//delete the user
	_, err = userMapper.Delete(&userModel)
	if err != nil {
		t.Errorf("Failed to delete user: %v", err)
	}

	//check whether user has really been deleted
	var userEmail, password, name, status, authToken, googleToken, facebookToken string
	var lastActivity time.Time

	queryErr := session.Query(`SELECT 
		user_email,
		password,
		name,
		status,
		last_activity,
		auth_token,
		google_token,
		facebook_token
		 FROM user WHERE user_email = 'user1@testEmail.com'`).
		Consistency(gocql.One).
		Scan(&userEmail,
			&password,
			&name,
			&status,
			&lastActivity,
			&authToken,
			&googleToken,
			&facebookToken)
	if queryErr == nil {
		t.Error("Error expected but got none")
	} else if "not found" != queryErr.Error() {
		t.Errorf("Got unexpected error: %v", err)
	}
	cleanupUserTable(t)
}

func TestFindById(t *testing.T) {
	initUserTable(t)
	userMapper := initUserMapperTest(t)

	var nowTime = time.Now()

	//initiate the model object to insert
	userModel := model.User{
		"user1@testEmail.com",
		"dummyPasswordHash",
		"user1",
		model.UserStatusActive,
		nowTime,
		"dummyAuthToken1",
		"dummyGoogleToken1",
		"dummyFacebookToken1"}

	//insert the models
	_, err := userMapper.Insert(&userModel)
	if err != nil {
		t.Errorf("Failed to insert user: %v", err)
	}

	//find the user by id
	foundModel, err := userMapper.FindByID(userModel.Email)
	if err != nil {
		t.Errorf("Failed to find by id: %v", err)
	}

	//check whether found user is correct
	if userModel.Email != foundModel.Email {
		t.Errorf("want %v for userEmail, got %v", userModel.Email, foundModel.Email)
	}
	if userModel.Password != foundModel.Password {
		t.Errorf("want %v for password, got %v", userModel.Password, foundModel.Password)
	}
	if userModel.Name != foundModel.Name {
		t.Errorf("want %v for name, got %v", userModel.Name, foundModel.Name)
	}
	if userModel.Status != foundModel.Status {
		t.Errorf("want %v for status, got %v", userModel.Status, foundModel.Status)
	}
	//Note on comparing time loaded from cassandra via gocql and time.Time instance created in code:
	//time.Time created in code has accuracy up to nanoseconds while time.Time loaded from gocql only has accuracy up to seconds
	//this can result in both time being unequal when compared, therefore we convert the time.TIme to unix timestamp prior to comparing
	if userModel.LastActivity.Unix() != foundModel.LastActivity.Unix() {
		t.Errorf("want %v for lastActivity, got %v", userModel.LastActivity.Unix(), foundModel.LastActivity.Unix())
	}
	if userModel.AuthToken != foundModel.AuthToken {
		t.Errorf("want %v for authToken, got %v", userModel.AuthToken, foundModel.AuthToken)
	}
	if userModel.GoogleToken != foundModel.GoogleToken {
		t.Errorf("want %v for googleToken, got %v", userModel.GoogleToken, foundModel.GoogleToken)
	}
	if userModel.FacebookToken != foundModel.FacebookToken {
		t.Errorf("want %v for facebookToken, got %v", userModel.FacebookToken, foundModel.FacebookToken)
	}
	cleanupUserTable(t)
}

func TestFindAllAndPage(t *testing.T) {
	initUserTable(t)
	userMapper := initUserMapperTest(t)

	var nowTime = time.Now()
	var userModelSlice []model.User

	//initiate the model objects to insert
	for i := 1; i <= 5; i++ {
		userModelSlice = append(userModelSlice, model.User{
			"user" + strconv.Itoa(i) + "@testEmail.com",
			"dummyPasswordHash",
			strconv.Itoa(i),
			model.UserStatusActive,
			nowTime,
			"dummyAuthToken" + strconv.Itoa(i),
			"dummyGoogleToken" + strconv.Itoa(i),
			"dummyFacebookToken" + strconv.Itoa(i)})
	}

	//insert the models
	for _, val := range userModelSlice {
		_, err := userMapper.Insert(&val)
		if err != nil {
			t.Errorf("Failed to insert user: %v", err)
		}
	}
	//find all models
	var foundModelSlice, allModelSlice []*model.User
	var err *errors.Error
	var isLast bool
	var pageSize = 2

	userMapper.SetPageSize(pageSize)
	foundModelSlice, err = userMapper.FindAll()
	//append foundModel to allModelSlice
	for _, foundModel := range foundModelSlice {
		allModelSlice = append(allModelSlice, foundModel)
	}
	for true {
		//reset slice
		foundModelSlice = foundModelSlice[:0]
		foundModelSlice, err, isLast = userMapper.NextPage()

		if err != nil {
			t.Errorf("nextPage call failed: %v", err)
			break
		}

		if len(foundModelSlice) > pageSize {
			t.Errorf("Expected returned slice length %v is more than page size %v", len(foundModelSlice), pageSize)
			break
		}

		//append foundModel to allModelSlice
		for _, foundModel := range foundModelSlice {
			allModelSlice = append(allModelSlice, foundModel)
		}

		if isLast {
			//done iterating resultset
			break
		}
	}

	//try calling nextPage again
	_, err, isLast = userMapper.NextPage()
	if err == nil {
		t.Errorf("error expected but got none")
	}
	if isLast == false {
		t.Errorf("want %v for isLast but got %v", true, isLast)
	}

	//check all appended model from result set so far
	if len(allModelSlice) != 5 {
		t.Errorf("want %v for allModelSlice length, got %v", 5, len(allModelSlice))
	}

	//confirm the found models
	for _, eachModel := range allModelSlice {
		counter := eachModel.Name
		if "user"+counter+"@testEmail.com" != eachModel.Email {
			t.Errorf("counter:%v, want %v for userEmail, got %v", counter, "user"+counter+"@testEmail.com", eachModel.Email)
		}
		if "dummyPasswordHash" != eachModel.Password {
			t.Errorf("counter:%v, want %v for password, got %v", counter, "dummyPasswordHash", eachModel.Password)
		}
		if counter != eachModel.Name {
			t.Errorf("counter:%v, want %v for name, got %v", counter, counter, eachModel.Name)
		}
		if model.UserStatusActive != eachModel.Status {
			t.Errorf("counter:%v, want %v for status, got %v", counter, model.UserStatusActive, eachModel.Status)
		}
		//Note on comparing time loaded from cassandra via gocql and time.Time instance created in code:
		//time.Time created in code has accuracy up to nanoseconds while time.Time loaded from gocql only has accuracy up to seconds
		//this can result in both time being unequal when compared, therefore we convert the time.TIme to unix timestamp prior to comparing
		if nowTime.Unix() != eachModel.LastActivity.Unix() {
			t.Errorf("counter:%v, want %v for lastActivity, got %v", counter, nowTime.String(), eachModel.LastActivity.String())
		}
		if "dummyAuthToken"+counter != eachModel.AuthToken {
			t.Errorf("counter:%v, want %v for authToken, got %v", counter, "dummyAuthToken"+counter, eachModel.AuthToken)
		}
		if "dummyGoogleToken"+counter != eachModel.GoogleToken {
			t.Errorf("counter:%v, want %v for googleToken, got %v", counter, "dummyGoogleToken"+counter, eachModel.GoogleToken)
		}
		if "dummyFacebookToken"+counter != eachModel.FacebookToken {
			t.Errorf("counter:%v, want %v for facebookToken, got %v", counter, "dummyFacebookToken"+counter, eachModel.FacebookToken)
		}
	}
	cleanupUserTable(t)
}
