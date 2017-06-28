package sample

import (
	"github.com/tehsphinx/elastic"
)

var (
	esIndex1 *eso.Index
)

// Init is to be called ONCE on app startup to initialize the structure
func Init() {
	// register the url
	eso.RegisterClient("db", "http://127.0.0.1:9200")

	// create all the indexes you need instantiating them once
	esIndex1 = eso.NewIndex("index1", "db")

	// optionally you can add settings and mappings to the index
	// Note: Only do this if you want to call esIndex1.CheckStructure()
	esIndex1.AddSettings(map[string]string{
		"index": `{
				"number_of_shards": 5,
				"number_of_replicas": 1
			}`,
	})
	esIndex1.AddMapping("docType1", "some docType mapping")

	// call to create the index with settings and mappings
	// Note: The index (with settings and mappings) is only created if it does not exist!
	esIndex1.CheckStructure()
}

// Define your docTypes and add creators for them
// By default a docType has the IndexDoc, Get, Delete and Search functionalities
// Add custom functions to your structs as needed
// or help expand the standard functionality by contributing to github.com/tehsphinx/elastic

func NewDocType1() *DocType1 {
	return &DocType1{
		DocType: eso.NewDocType(esIndex1, "docType1"),
	}
}

type DocType1 struct {
	eso.DocType
}
