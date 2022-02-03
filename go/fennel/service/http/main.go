package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"fennel/controller/action"
	aggregate2 "fennel/controller/aggregate"
	profile2 "fennel/controller/profile"
	"fennel/db"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/kafka"
	actionlib "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	httplib "fennel/lib/http"
	profilelib "fennel/lib/profile"
	"fennel/lib/query"
	"fennel/lib/value"
	"fennel/plane"
	"fennel/redis"

	"github.com/alexflint/go-arg"
	"google.golang.org/protobuf/proto"
)

// Flags for the API server.
var args struct {
	KafkaServer   string `arg:"--kafka-server,env:KAFKA_SERVER_ADDRESS"`
	KafkaUsername string `arg:"--kafka-user,env:KAFKA_USERNAME"`
	KafkaPassword string `arg:"--kafka-password,env:KAFKA_PASSWORD"`

	MysqlHost     string `arg:"--mysql-host,env:MYSQL_SERVER_ADDRESS"`
	MysqlDB       string `arg:"--mysql-db,env:MYSQL_DATABASE_NAME"`
	MysqlUsername string `arg:"--mysql-user,env:MYSQL_USERNAME"`
	MysqlPassword string `arg:"--mysql-password,env:MYSQL_PASSWORD"`

	RedisServer string `arg:"--redis-server,env:REDIS_SERVER_ADDRESS"`
}

type holder struct {
	plane plane.Plane
}

func parse(req *http.Request, msg proto.Message) error {
	defer req.Body.Close()
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return err
	}
	return proto.Unmarshal(body, msg)
}

func (m holder) Log(w http.ResponseWriter, req *http.Request) {
	var pa actionlib.ProtoAction
	if err := parse(req, &pa); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a := actionlib.FromProtoAction(&pa)
	// fwd to controller

	aid, err := action.Insert(m.plane, a)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// write the actionID back
	fmt.Fprintf(w, fmt.Sprintf("%d", aid))
}

func (m holder) Fetch(w http.ResponseWriter, req *http.Request) {
	var protoRequest actionlib.ProtoActionFetchRequest
	if err := parse(req, &protoRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := actionlib.FromProtoActionFetchRequest(&protoRequest)
	// send to controller
	actions, err := action.Fetch(m.plane, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	actionList := actionlib.ToProtoActionList(actions)
	ser, err := proto.Marshal(actionList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Write(ser)
}

func (m holder) GetProfile(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	// send to controller
	val, err := profile2.Get(m.plane, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if val == nil {
		// no error but no value to return either, so we just write nothing and client knows that
		// empty response means no value
		fmt.Fprintf(w, string(""))
		return
	}
	// now convert value to proto and serialize it
	pval, err := value.ToProtoValue(val)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	valueSer, err := proto.Marshal(&pval)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Write(valueSer)
}

// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't Run into a race condition
func (m holder) SetProfile(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	// send to controller
	if err = profile2.Set(m.plane, request); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
}

func (m holder) GetProfiles(w http.ResponseWriter, req *http.Request) {
	var protoRequest profilelib.ProtoProfileFetchRequest
	if err := parse(req, &protoRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := profilelib.FromProtoProfileFetchRequest(&protoRequest)
	// send to controller
	profiles, err := profile2.GetProfiles(m.plane, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	profileList, err := profilelib.ToProtoProfileList(profiles)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ser, err := proto.Marshal(profileList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Write(ser)
}

func (m holder) Query(w http.ResponseWriter, req *http.Request) {
	var protoAstWithDict query.ProtoAstWithDict
	if err := parse(req, &protoAstWithDict); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tree, dict, err := query.FromProtoAstWithDict(&protoAstWithDict)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// execute the tree
	i := interpreter.NewInterpreter(bootarg.Create(m.plane))
	i.SetQueryArgs(dict)
	ret, err := tree.AcceptValue(i)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	pval, err := value.ToProtoValue(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ser, err := proto.Marshal(&pval)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Write(ser)
}

func (m holder) StoreAggregate(w http.ResponseWriter, req *http.Request) {
	var protoAgg aggregate.ProtoAggregate
	if err := parse(req, &protoAgg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	agg, err := aggregate.FromProtoAggregate(protoAgg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// call controller
	if err = aggregate2.Store(m.plane, agg); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
}

func (m holder) RetrieveAggregate(w http.ResponseWriter, req *http.Request) {
	var protoReq aggregate.AggRequest
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// call controller
	ret, err := aggregate2.Retrieve(m.plane, ftypes.AggType(protoReq.AggType), ftypes.AggName(protoReq.AggName))
	if err == aggregate.ErrNotFound {
		// we don't throw an error, just return empty response
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// to send ret back, we will convert it to proto, marshal it and then write it back
	protoRet, err := aggregate.ToProtoAggregate(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ser, err := proto.Marshal(&protoRet)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Write(ser)
}

func (m holder) AggregateValue(w http.ResponseWriter, req *http.Request) {
	var protoReq aggregate.ProtoGetAggValueRequest
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	getAggValue, err := aggregate.FromProtoGetAggValueRequest(&protoReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// call controller
	ret, err := aggregate2.Value(m.plane, getAggValue.AggType, getAggValue.AggName, getAggValue.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// marshal ret and then write it back
	ser, err := value.Marshal(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Write(ser)
}

func setHandlers(controller holder, mux *http.ServeMux) {
	mux.HandleFunc("/fetch", controller.Fetch)
	mux.HandleFunc("/get", controller.GetProfile)
	mux.HandleFunc("/set", controller.SetProfile)
	mux.HandleFunc("/log", controller.Log)
	mux.HandleFunc("/get_profiles", controller.GetProfiles)
	mux.HandleFunc("/query", controller.Query)
	mux.HandleFunc("/store_aggregate", controller.StoreAggregate)
	mux.HandleFunc("/retrieve_aggregate", controller.RetrieveAggregate)
	mux.HandleFunc("/aggregate_value", controller.AggregateValue)
}

func createPlane() (*plane.Plane, error) {
	mysqlConfig := db.MySQLConfig{
		Host:     args.MysqlHost,
		DBname:   args.MysqlDB,
		Username: args.MysqlUsername,
		Password: args.MysqlPassword,
	}
	sqlConn, err := mysqlConfig.Materialize()
	if err != nil {
		return nil, fmt.Errorf("failed to connect with mysql: %v", err)
	}

	redisConfig := redis.ClientConfig{
		Addr:      args.RedisServer,
		TLSConfig: &tls.Config{},
	}
	redisClient, err := redisConfig.Materialize()
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %v", err)
	}

	kafkaConsumerConfig := kafka.RemoteConsumerConfig{
		BootstrapServer: args.KafkaServer,
		Username:        args.KafkaUsername,
		Password:        args.KafkaPassword,
		// TODO: add topic id, group id, and offset policy.
		GroupID:      "",
		Topic:        "",
		OffsetPolicy: "",
	}
	kafkaConsumer, err := kafkaConsumerConfig.Materialize()
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}

	kafkaProducerConfig := kafka.RemoteProducerConfig{
		BootstrapServer: args.KafkaServer,
		Username:        args.KafkaUsername,
		Password:        args.KafkaPassword,
		// TODO: add topic id
		Topic:         "",
		RecreateTopic: false,
	}
	kafkaProducer, err := kafkaProducerConfig.Materialize()
	if err != nil {
		return nil, fmt.Errorf("failed to crate kafka producer: %v", err)
	}

	return &plane.Plane{
		DB:             sqlConn.(db.Connection),
		Redis:          redisClient.(redis.Client),
		ActionConsumer: kafkaConsumer.(kafka.RemoteConsumer),
		ActionProducer: kafkaProducer.(kafka.RemoteProducer),
		Clock:          clock.Unix{},
		// TODO: Replace with actual ids.
		CustID: ftypes.CustID(1),
		TierID: ftypes.TierID(1),
		ID:     ftypes.PlaneID(1),
		// TODO: add client to ElasticCache-backed Redis instead of MemoryDB.
		Cache: redis.NewCache(redisClient.(redis.Client)),
	}, nil

}

func main() {
	// Parse flags / environment variables.
	arg.MustParse(&args)

	// spin up http service
	server := &http.Server{Addr: fmt.Sprintf(":%d", httplib.PORT)}
	mux := http.NewServeMux()
	plane, err := createPlane()
	if err != nil {
		panic(fmt.Sprintf("Failed to setup plane connectors: %v", err))

	}
	controller := holder{*plane}
	setHandlers(controller, mux)
	server.Handler = mux
	log.Printf("starting http service on %s...", server.Addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		// unexpected error. port in use?
		log.Fatalf("ListenAndServe(): %v", err)
	}
}
