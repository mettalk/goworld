package mongodbmongodriver

import (
	"context"
	"io"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/xiaonanln/goworld/engine/common"
	"github.com/xiaonanln/goworld/engine/gwlog"
	storagecommon "github.com/xiaonanln/goworld/engine/storage/storage_common"
)

const (
	_DEFAULT_DB_NAME = "goworld"
)

type mongoDBEntityStorge struct {
	client *mongo.Client
	dbname string
}

// OpenMongoDB opens mongodb as entity storage
func OpenMongoDB(url string, dbname string) (storagecommon.EntityStorage, error) {
	gwlog.Debugf("Connecting MongoDB ...")
	client, err := mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if dbname == "" {
		// if db is not specified, use default
		dbname = _DEFAULT_DB_NAME
	}
	return &mongoDBEntityStorge{
		client: client,
		dbname: dbname,
	}, nil
}

func (es *mongoDBEntityStorge) Write(typeName string, entityID common.EntityID, data interface{}) error {
	replaceOpts := options.Replace().SetUpsert(true)
	filter := bson.M{"_id": entityID}
	replacement := bson.M{
		"data": data,
	}
	_, err := es.client.Database(es.dbname).Collection(typeName).ReplaceOne(context.TODO(), filter, replacement, replaceOpts)
	return err
}

func (es *mongoDBEntityStorge) Read(typeName string, entityID common.EntityID) (interface{}, error) {
	var doc bson.M
	findOneOpts := options.FindOne().SetMaxTime(10 * time.Second)
	err := es.client.Database(es.dbname).Collection(typeName).FindOne(context.TODO(), bson.M{
		"_id": entityID,
	}, findOneOpts).Decode(&doc)
	if err != nil {
		return nil, err
	}
	return es.convertM2Map(doc["data"].(bson.M)), nil
}

func (es *mongoDBEntityStorge) convertM2Map(m bson.M) map[string]interface{} {
	ma := map[string]interface{}(m)
	es.convertM2MapInMap(ma)
	return ma
}

func (es *mongoDBEntityStorge) convertM2MapInMap(m map[string]interface{}) {
	for k, v := range m {
		switch im := v.(type) {
		case bson.M:
			m[k] = es.convertM2Map(im)
		case map[string]interface{}:
			es.convertM2MapInMap(im)
		case []interface{}:
			es.convertM2MapInList(im)
		}
	}
}

func (es *mongoDBEntityStorge) convertM2MapInList(l []interface{}) {
	for i, v := range l {
		switch im := v.(type) {
		case bson.M:
			l[i] = es.convertM2Map(im)
		case map[string]interface{}:
			es.convertM2MapInMap(im)
		case []interface{}:
			es.convertM2MapInList(im)
		}
	}
}

func (es *mongoDBEntityStorge) List(typeName string) ([]common.EntityID, error) {
	var docs []bson.M
	findOpts := options.Find().SetMaxTime(10 * time.Second)
	cursor, err := es.client.Database(es.dbname).Collection(typeName).Find(context.TODO(), bson.M{}, findOpts)
	if err != nil {
		return nil, err
	}
	if err := cursor.All(context.TODO(), &docs); err != nil {
		return nil, err
	}

	entityIDs := make([]common.EntityID, len(docs))
	for i, doc := range docs {
		entityIDs[i] = common.EntityID(doc["_id"].(string))
	}
	return entityIDs, nil

}

func (es *mongoDBEntityStorge) Exists(typeName string, entityID common.EntityID) (bool, error) {
	var doc bson.M
	findOneOpts := options.FindOne().SetMaxTime(10 * time.Second)
	err := es.client.Database(es.dbname).Collection(typeName).FindOne(context.TODO(), bson.M{
		"_id": entityID,
	}, findOneOpts).Decode(&doc)
	if err == nil {
		return true, nil
	} else if err == mongo.ErrNoDocuments {
		return false, nil
	}
	return false, err
}

func (es *mongoDBEntityStorge) Close() {
	es.client.Disconnect(context.TODO())
}

func (es *mongoDBEntityStorge) IsEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF
}
