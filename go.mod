module github.com/couchbase/n1k1

go 1.17

replace (
	github.com/couchbase/cbft => ./tmp/master/cbft
	github.com/couchbase/cbftx => ./tmp/master/cbftx
	github.com/couchbase/cbgt => ./tmp/master/cbgt
	github.com/couchbase/eventing-ee => ./tmp/master-goproj-couchbase/eventing-ee
	github.com/couchbase/go_json => ./tmp/master-goproj-couchbase/go_json
	github.com/couchbase/indexing => ./tmp/master-goproj-couchbase/indexing
	github.com/couchbase/n1fty => ./tmp/master-goproj-couchbase/n1fty
	github.com/couchbase/query => ./tmp/master-goproj-couchbase/query
	github.com/couchbase/query-ee => ./tmp/master-goproj-couchbase/query-ee
)

require (
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/query v0.0.0-20211130220939-72c95b9baa70
	github.com/couchbase/rhmap v0.0.0-20200512125128-60fa597d6dd1
)

require (
	github.com/RoaringBitmap/roaring v0.9.4 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/blevesearch/bleve-mapping-ui v0.4.0 // indirect
	github.com/blevesearch/bleve/v2 v2.2.2 // indirect
	github.com/blevesearch/bleve_index_api v1.0.1 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/mmap-go v1.0.3 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.1.0 // indirect
	github.com/blevesearch/sear v0.0.4 // indirect
	github.com/blevesearch/segment v0.9.0 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.1 // indirect
	github.com/blevesearch/vellum v1.0.7 // indirect
	github.com/blevesearch/zapx/v11 v11.3.1 // indirect
	github.com/blevesearch/zapx/v12 v12.3.1 // indirect
	github.com/blevesearch/zapx/v13 v13.3.1 // indirect
	github.com/blevesearch/zapx/v14 v14.3.1 // indirect
	github.com/blevesearch/zapx/v15 v15.3.1 // indirect
	github.com/couchbase/blance v0.1.1 // indirect
	github.com/couchbase/cbauth v0.1.0 // indirect
	github.com/couchbase/cbft v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/ghistogram v0.1.0 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/go_json v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/gocbcore-transactions v0.0.0-20211015175912-8d8655302661 // indirect
	github.com/couchbase/gocbcore/v10 v10.0.5 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.7 // indirect
	github.com/couchbase/gomemcached v0.1.4 // indirect
	github.com/couchbase/gometa v0.0.0-20200717102231-b0e38b71d711 // indirect
	github.com/couchbase/goutils v0.1.2 // indirect
	github.com/couchbase/indexing v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/moss v0.1.0 // indirect
	github.com/couchbase/n1fty v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbasedeps/go-curl v0.0.0-20190830233031-f0b2afc926ec // indirect
	github.com/dustin/go-jsonpointer v0.0.0-20140810065344-75939f54b39e // indirect
	github.com/dustin/gojson v0.0.0-20150115165335-af16e0e771e2 // indirect
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/elazarl/go-bindata-assetfs v1.0.1 // indirect
	github.com/golang/protobuf v1.4.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/steveyen/gtreap v0.1.0 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/sys v0.0.0-20211124211545-fe61309f8881 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	google.golang.org/grpc v1.24.0 // indirect
	google.golang.org/protobuf v1.21.0 // indirect
)
