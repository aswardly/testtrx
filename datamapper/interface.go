//Package datamapper provides the definitions of data mapper
package datamapper

import (
	"testtrx/model"

	"github.com/go-errors/errors"
)

//TimeFormat is a const of string for parsing string to time or formatting time instance to string
//Note: watch for precision of seconds (cassandra can only support precision up to seconds)
const TimeFormat = "2006-01-02 15:04:05 +0000 UTC"

//DataMapper is an interface for data mapper
type DataMapper interface {
	FindById(id string) (*model.Model, *errors.Error)
	FindAll() ([]*model.Model, *errors.Error)
	Insert(model *model.Model) (bool, *errors.Error)
	Update(model *model.Model) (bool, *errors.Error)
	Delete(model *model.Model) (bool, *errors.Error)
}
