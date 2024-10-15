package scalers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type mongoDBScaler struct {
	metricType v2.MetricTargetType
	metadata   mongoDBMetadata
	client     *mongo.Client
	logger     logr.Logger
}

type mongoDBMetadata struct {
	ConnectionString     string `keda:"name=connectionString,order=authParams;triggerMetadata"`
	Scheme               string `keda:"name=scheme,order=authParams;triggerMetadata,default=mongodb"`
	Host                 string `keda:"name=host,order=authParams;triggerMetadata"`
	Port                 string `keda:"name=port,order=authParams;triggerMetadata"`
	Username             string `keda:"name=username,order=authParams;triggerMetadata"`
	Password             string `keda:"name=password,order=authParams;triggerMetadata"`
	DbName               string `keda:"name=dbName,order=authParams;triggerMetadata"`
	Collection           string `keda:"name=collection,order=triggerMetadata"`
	Query                string `keda:"name=query,order=triggerMetadata"`
	QueryValue           int64  `keda:"name=queryValue,order=triggerMetadata"`
	ActivationQueryValue int64  `keda:"name=activationQueryValue,order=triggerMetadata,optional"`
	TriggerIndex         int
}

func (m *mongoDBMetadata) Validate() error {
	if m.ConnectionString == "" {
		if m.Host == "" {
			return fmt.Errorf("no host given")
		}
		if m.Username == "" {
			return fmt.Errorf("no username given")
		}
		if m.Password == "" {
			return fmt.Errorf("no password given")
		}
		if m.DbName == "" {
			return fmt.Errorf("no dbName given")
		}
		if !strings.Contains(m.Scheme, "mongodb+srv") && m.Port == "" {
			return fmt.Errorf("no port given")
		}
	}
	return nil
}

const (
	mongoDBDefaultTimeOut = 10 * time.Second
)

func NewMongoDBScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	meta, err := parseMongoDBMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("failed to parsing mongoDB metadata: %w", err)
	}

	connStr, err := getConnectionString(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection string: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, mongoDBDefaultTimeOut)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		return nil, fmt.Errorf("failed to establish connection with mongoDB: %w", err)
	}

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping mongoDB: %w", err)
	}

	return &mongoDBScaler{
		metricType: metricType,
		metadata:   meta,
		client:     client,
		logger:     InitializeLogger(config, "mongodb_scaler"),
	}, nil
}

func parseMongoDBMetadata(config *scalersconfig.ScalerConfig) (mongoDBMetadata, error) {
	meta := mongoDBMetadata{}
	err := config.TypedConfig(&meta)
	if err != nil {
		return meta, fmt.Errorf("failed to parse metadata: %w", err)
	}

	meta.TriggerIndex = config.TriggerIndex
	return meta, nil
}

func getConnectionString(meta mongoDBMetadata) (string, error) {
	if meta.ConnectionString != "" {
		return meta.ConnectionString, nil
	}

	if meta.Scheme == "mongodb+srv" {
		return fmt.Sprintf("%s://%s:%s@%s/%s",
			meta.Scheme,
			url.QueryEscape(meta.Username),
			url.QueryEscape(meta.Password),
			meta.Host,
			meta.DbName), nil
	}

	addr := net.JoinHostPort(meta.Host, meta.Port)
	return fmt.Sprintf("%s://%s:%s@%s/%s",
		meta.Scheme,
		url.QueryEscape(meta.Username),
		url.QueryEscape(meta.Password),
		addr,
		meta.DbName), nil
}

func (s *mongoDBScaler) Close(ctx context.Context) error {
	if s.client != nil {
		err := s.client.Disconnect(ctx)
		if err != nil {
			s.logger.Error(err, "failed to close mongoDB connection")
			return err
		}
	}
	return nil
}

func (s *mongoDBScaler) getQueryResult(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, mongoDBDefaultTimeOut)
	defer cancel()

	filter, err := json2BsonDoc(s.metadata.Query)
	if err != nil {
		return 0, fmt.Errorf("failed to convert query param to bson.Doc: %w", err)
	}

	docsNum, err := s.client.Database(s.metadata.DbName).Collection(s.metadata.Collection).CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to query %v in %v: %w", s.metadata.DbName, s.metadata.Collection, err)
	}

	return docsNum, nil
}

func (s *mongoDBScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	num, err := s.getQueryResult(ctx)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("failed to inspect mongoDB: %w", err)
	}

	metric := GenerateMetricInMili(metricName, float64(num))

	return []external_metrics.ExternalMetricValue{metric}, num > s.metadata.ActivationQueryValue, nil
}

func (s *mongoDBScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.TriggerIndex, kedautil.NormalizeString(fmt.Sprintf("mongodb-%s", s.metadata.Collection))),
		},
		Target: GetMetricTarget(s.metricType, s.metadata.QueryValue),
	}
	metricSpec := v2.MetricSpec{
		External: externalMetric, Type: externalMetricType,
	}
	return []v2.MetricSpec{metricSpec}
}

func json2BsonDoc(js string) (doc bson.D, err error) {
	doc = bson.D{}
	err = bson.UnmarshalExtJSON([]byte(js), true, &doc)
	if err != nil {
		return nil, err
	}

	if len(doc) == 0 {
		return nil, errors.New("empty bson document")
	}

	return doc, nil
}
