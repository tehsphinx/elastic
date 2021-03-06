package eso

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"gopkg.in/olivere/elastic.v5"
)

var (
	clients = map[string]*client{}
	urls    = map[string]string{}
)

func RegisterClient(name, url string) {
	urls[name] = url
}

func newClient(name string) *client {
	conn, ok := clients[name]
	if !ok {
		url, ok := urls[name]
		if !ok {
			log.Fatal(fmt.Sprintf("unknown elasticsearch client %s", name))
		}

		conn = &client{name: name, url: url}
		clients[name] = conn
		conn.checkConn()
	}

	return conn
}

type client struct {
	name string
	url  string
	conn *elastic.Client
}

func (s *client) checkConn() error {
	var err error
	if s.conn == nil {
		err = s.newConn()
		if err != nil {
			log.Fatal(err)
		}
	}
	return err
}

func (s *client) newConn() error {
	log.Printf("Opening new Elastic connection to %s called '%s'", s.url, s.name)
	cl, err := elastic.NewSimpleClient(elastic.SetURL(s.url),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetInfoLog(log.New(ioutil.Discard, "", log.LstdFlags)))
	s.conn = cl
	return err
}

func NewIndex(name, db string) *Index {
	cl := newClient(db)
	return &Index{
		cl:       cl,
		name:     name,
		settings: map[string]string{},
		mappings: map[string]string{},
	}
}

type Index struct {
	cl       *client
	name     string
	settings map[string]string
	mappings map[string]string
}

func (s *Index) CheckStructure() error {
	exists, err := s.indexExists(s.name)
	if err == nil && !exists {
		err = s.CreateIndex(s.name)
	}
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (s *Index) indexExists(index string) (bool, error) {
	return s.cl.conn.IndexExists(index).Do(context.TODO())
}

func (s *Index) AddMapping(docType, mapping string) {
	s.mappings[docType] = mapping
}

func (s *Index) AddSetting(key, settings string) {
	s.settings[key] = settings
}

// CreateIndex creates an index by name. The index specified in the struct is created anyway if it doesnt exist.
func (s *Index) CreateIndex(index string) error {
	body := fmt.Sprintf(`{"settings": %s, "mappings": %s}`,
		formatMapOfStrings(s.settings),
		formatMapOfStrings(s.mappings))

	createIndex, err := s.cl.conn.CreateIndex(index).Body(body).Do(context.TODO())
	if err == nil && !createIndex.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge new index")
	}
	return err
}

// DeleteIndex deletes the index specified in the struct.
func (s *Index) DeleteIndex(index string) error {
	deleteIndex, err := s.cl.conn.DeleteIndex(index).Do(context.TODO())
	if err == nil && !deleteIndex.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge deletion of index")
	}
	return err
}

func (s *Index) PutIndexTemplate(name string, body string) error {
	res, err := s.cl.conn.IndexPutTemplate(name).BodyString(body).Do(context.TODO())
	if err == nil && !res.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge creation of template")
	}
	return err
}

func (s *Index) DeleteIndexTemplate(name string) error {
	res, err := s.cl.conn.IndexDeleteTemplate(name).Do(context.TODO())
	if err == nil && !res.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge deletion of tempate")
	}
	return err
}

func NewDocType(index *Index, name string) *DocType {
	return &DocType{
		Index: index,
		name:  name,
	}
}

type DocType struct {
	*Index
	name string
}

// IndexDoc creates a document in elasticsearch
func (s *DocType) IndexDoc(doc interface{}, id string) (string, error) {
	var (
		body string
		ok   bool
	)
	if body, ok = doc.(string); !ok {
		d, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		body = string(d)
	}

	q := s.cl.conn.Index().Index(s.Index.name).Type(s.name).BodyJson(body)
	if id != "" {
		q = q.Id(id)
	}

	res, err := q.Do(context.TODO())
	if err != nil {
		return "", err
	}
	return res.Id, err
}

// Get retrieves a document from elasticsearch by id
func (s *DocType) Get(id string) (*elastic.GetResult, error) {
	res, err := s.cl.conn.Get().Index(s.Index.name).Type(s.name).Id(id).Do(context.TODO())
	return res, err
}

// Delete removes one document from elasticsearch by id
func (s *DocType) Delete(id string) (bool, error) {
	res, err := s.cl.conn.Delete().Index(s.Index.name).Type(s.name).Id(id).Do(context.TODO())
	return res.Found, err
}

// Search takes a json search string and executes it, returning the result
func (s *DocType) Search(json interface{}) (*elastic.SearchResult, error) {
	return s.cl.conn.Search(s.Index.name).Source(json).Pretty(true).Do(context.TODO())
}

func formatMapOfStrings(m map[string]string) string {
	s := fmt.Sprintf("%#v", m)
	s = strings.Replace(s, "\\\"", "\"", -1)
	s = strings.Replace(s, "\"{", "{", -1)
	s = strings.Replace(s, "}\"", "}", -1)
	s = strings.Replace(s, "\\n", "", -1)
	s = strings.Replace(s, "\\t", "", -1)
	return strings.Trim(s, "map[string]")
}

func NewDoc(docType *DocType) *Doc {
	return &Doc{
		DocType: docType,
	}
}

type Doc struct {
	DocType *DocType `json:"-"`
	ID      string   `json:"-"`
}

func (s *Doc) Save(doc interface{}) error {
	id, err := s.DocType.IndexDoc(doc, s.ID)
	if err != nil {
		return err
	}
	s.ID = id
	return nil
}

func (s *Doc) FillByID(target interface{}, id string) error {
	res, err := s.DocType.Get(id)
	if err != nil {
		return err
	}

	if res.Source == nil {
		return errors.New("empty source returned")
	}

	return json.Unmarshal([]byte(*res.Source), target)
}

func (s *Doc) Delete() (bool, error) {
	return s.DocType.Delete(s.ID)
}
