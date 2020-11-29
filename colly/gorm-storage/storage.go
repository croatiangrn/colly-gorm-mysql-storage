package gorm_storage

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"log"
	"net/url"
	"strconv"
)

// Storage implements a PostgreSQL storage backend for colly
type Storage struct {
	VisitedTable string
	CookiesTable string
	db           *gorm.DB
}

func NewStorage(visitedTable string, cookiesTable string, db *gorm.DB) *Storage {
	storage := &Storage{VisitedTable: visitedTable, CookiesTable: cookiesTable, db: db}
	_ = storage.Init()
	return storage
}

// Init initializes the PostgreSQL storage
func (s *Storage) Init() error {
	var err error

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (request_id text not null, timestamp datetime not null default current_timestamp);", s.VisitedTable)

	if err = s.db.Exec(query).Error; err != nil {
		log.Fatal(err)
	}

	if len(s.CookiesTable) > 0 {
		query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (host text not null, cookies text not null);", s.CookiesTable)

		if err = s.db.Exec(query).Error; err != nil {
			log.Fatal(err)
		}
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
	err := s.db.Raw(query, strconv.FormatUint(requestID, 10)).Row().Scan(&isVisited)
	if err != nil {
		log.Println(err)
	}

	return isVisited, err
}

// Cookies implements colly/storage.Cookies()
func (s *Storage) Cookies(u *url.URL) string {
	var cookies string

	query := fmt.Sprintf(`SELECT cookies FROM %s WHERE host = ?;`, s.CookiesTable)

	if err := s.db.Raw(query, u.Host).Row().Scan(&cookies); err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Fatal(err)
	}

	return cookies
}

// SetCookies implements colly/storage.SetCookies()
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	query := fmt.Sprintf(`INSERT INTO %s (host, cookies) VALUES(?, ?);`, s.CookiesTable)

	s.db.Exec(query, u.Host, cookies)
}

// Clear clears storage
func (s *Storage) Clear() error {
	visitedQuery := fmt.Sprintf(`DELETE FROM %s WHERE 1=1`, s.VisitedTable)
	cookiesQuery := fmt.Sprintf(`DELETE FROM %s WHERE 1=1`, s.CookiesTable)

	if err := s.db.Exec(visitedQuery).Error; err != nil {
		return err
	}

	if err := s.db.Exec(cookiesQuery).Error; err != nil {
		return err
	}

	return nil
}