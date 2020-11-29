package gorm_storage

import (
	"errors"
	"fmt"
	rndCommon "github.com/croatiangrn/go-rnd-common"
	"github.com/jinzhu/gorm"
	"net/url"
	"strconv"
)

var (
	ErrDBError = errors.New("db_err")
)

// Storage implements a PostgreSQL storage backend for colly
type Storage struct {
	VisitedTable string
	CookiesTable string
	db           *gorm.DB
}

func NewStorage(visitedTable string, cookiesTable string, db *gorm.DB) *Storage {
	return &Storage{VisitedTable: visitedTable, CookiesTable: cookiesTable, db: db}
}

// Init initializes the PostgreSQL storage
func (s *Storage) Init() error {
	var err error

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (request_id text not null);", s.VisitedTable)

	if err = s.db.Exec(query).Error; err != nil {
		rndCommon.LogError(err)
		return ErrDBError
	}

	query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (host text not null, cookies text not null);", s.CookiesTable)

	if err = s.db.Exec(query).Error; err != nil {
		rndCommon.LogError(err)
		return ErrDBError
	}

	return nil

}

// Visited implements colly/storage.Visited()
func (s *Storage) Visited(requestID uint64) error {
	query := fmt.Sprintf(`INSERT INTO %s (request_id) VALUES(?);`, s.VisitedTable)

	return s.db.Exec(query, strconv.FormatUint(requestID, 10)).Error
}

// IsVisited implements colly/storage.IsVisited()
func (s *Storage) IsVisited(requestID uint64) (bool, error) {
	var isVisited bool

	query := fmt.Sprintf(`SELECT EXISTS(SELECT request_id FROM %s WHERE request_id = ?)`, s.VisitedTable)

	err := s.db.Raw(query, strconv.FormatUint(requestID, 10)).Scan(&isVisited).Error

	return isVisited, err
}

// Cookies implements colly/storage.Cookies()
func (s *Storage) Cookies(u *url.URL) string {
	var cookies string

	query := fmt.Sprintf(`SELECT cookies FROM %s WHERE host = ?;`, s.CookiesTable)

	s.db.Raw(query, u.Host).Scan(&cookies)

	return cookies
}

// SetCookies implements colly/storage.SetCookies()
func (s *Storage) SetCookies(u *url.URL, cookies string) {

	query := fmt.Sprintf(`INSERT INTO %s (host, cookies) VALUES(?, ?);`, s.CookiesTable)

	s.db.Exec(query, u.Host, cookies)
}
