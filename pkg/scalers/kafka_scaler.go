/*
Copyright 2023 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This scaler is based on sarama library.
// It lacks support for AWS MSK. For AWS MSK please see: apache-kafka scaler.

package scalers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	"github.com/go-logr/logr"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	awsutils "github.com/kedacore/keda/v2/pkg/scalers/aws"
	"github.com/kedacore/keda/v2/pkg/scalers/kafka"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type kafkaScaler struct {
	metricType      v2.MetricTargetType
	metadata        *kafkaMetadata
	client          sarama.Client
	admin           sarama.ClusterAdmin
	logger          logr.Logger
	previousOffsets map[string]map[int32]int64
}

const (
	stringEnable     = "enable"
	stringDisable    = "disable"
	defaultUnsafeSsl = false
	invalidOffset    = -1
)

type kafkaMetadata struct {
	triggerIndex int

	// If an invalid offset is found, whether to scale to 1 (false - the default) so consumption can
	// occur or scale to 0 (true). See discussion in https://github.com/kedacore/keda/issues/2612

	BootstrapServers                   []string `keda:"name=bootstrapServers,order=triggerMetadata;resolvedEnv"`
	Group                              string   `keda:"name=consumerGroup,order=triggerMetadata;resolvedEnv"`
	Topic                              string   `keda:"name=topic,order=triggerMetadata;resolvedEnv,optional"`
	PartitionLimitation                []int32  `keda:"name=partitionLimitation,order=triggerMetadata,optional"`
	LagThreshold                       int64    `keda:"name=lagThreshold,order=triggerMetadata,default=10"`
	ActivationLagThreshold             int64    `keda:"name=activationLagThreshold,order=triggerMetadata,default=0"`
	OffsetResetPolicy                  string   `keda:"name=offsetResetPolicy,order=triggerMetadata,default=latest,enum=latest;earliest"`
	AllowIdleConsumers                 bool     `keda:"name=allowIdleConsumers,order=triggerMetadata,default=false"`
	ExcludePersistentLag               bool     `keda:"name=excludePersistentLag,order=triggerMetadata,default=false"`
	ScaleToZeroOnInvalidOffset         bool     `keda:"name=scaleToZeroOnInvalidOffset,order=triggerMetadata,default=false"`
	LimitToPartitionsWithLag           bool     `keda:"name=limitToPartitionsWithLag,order=triggerMetadata,default=false"`
	EnsureEvenDistributionOfPartitions bool     `keda:"name=ensureEvenDistributionOfPartitions,order=triggerMetadata,default=false"`
	Version                            string   `keda:"name=version,order=triggerMetadata,default=1.0.0"`

	SASL     string `keda:"name=sasl,order=triggerMetadata;authParams,optional,enum=none;plaintext;scram_sha256;scram_sha512;oauthbearer;gssapi"`
	Username string `keda:"name=username,order=authParams,optional"`
	Password string `keda:"name=password,order=authParams,optional"`

	Keytab              string `keda:"name=keytab,order=authParams,optional"`
	Realm               string `keda:"name=realm,order=authParams,optional"`
	KerberosConfig      string `keda:"name=kerberosConfig,order=authParams,optional"`
	KerberosServiceName string `keda:"name=kerberosServiceName,order=authParams,optional"`
	KerberosDisableFAST bool   `keda:"name=kerberosDisableFAST,order=authParams,default=false"`

	SAMLTokenProvider     string `keda:"name=saslTokenProvider,order=triggerMetadata;authParams,optional"`
	Scopes                string `keda:"name=scopes,order=authParams,optional"`
	OAuthTokenEndpointURI string `keda:"name=oauthTokenEndpointUri,order=authParams,optional"`
	OAuthExtensions       string `keda:"name=oauthExtensions,order=authParams,optional"`

	AWSRegion string `keda:"name=awsRegion,order=triggerMetadata,optional"`

	TLS         string `keda:"name=tls,order=triggerMetadata;authParams,optional,enum=enable;disable"`
	Cert        string `keda:"name=cert,order=authParams,optional"`
	Key         string `keda:"name=key,order=authParams,optional"`
	KeyPassword string `keda:"name=keyPassword,order=authParams,optional"`
	CA          string `keda:"name=ca,order=authParams,optional"`
	UnsafeSsl   bool   `keda:"name=unsafeSsl,order=triggerMetadata,default=false"`

	offsetResetPolicy  offsetResetPolicy
	saslType           kafkaSaslType
	tokenProvider      kafkaSaslOAuthTokenProvider
	scopes             []string
	oauthExtensions    map[string]string
	awsAuthorization   awsutils.AuthorizationMetadata
	enableTLS          bool
	version            sarama.KafkaVersion
	keytabPath         string
	kerberosConfigPath string
}

type offsetResetPolicy string

const (
	latest   offsetResetPolicy = "latest"
	earliest offsetResetPolicy = "earliest"
)

type kafkaSaslType string

// supported SASL types
const (
	KafkaSASLTypeNone        kafkaSaslType = "none"
	KafkaSASLTypePlaintext   kafkaSaslType = "plaintext"
	KafkaSASLTypeSCRAMSHA256 kafkaSaslType = "scram_sha256"
	KafkaSASLTypeSCRAMSHA512 kafkaSaslType = "scram_sha512"
	KafkaSASLTypeOAuthbearer kafkaSaslType = "oauthbearer"
	KafkaSASLTypeGSSAPI      kafkaSaslType = "gssapi"
)

type kafkaSaslOAuthTokenProvider string

// supported SASL OAuth token provider types
const (
	KafkaSASLOAuthTokenProviderBearer    kafkaSaslOAuthTokenProvider = "bearer"
	KafkaSASLOAuthTokenProviderAWSMSKIAM kafkaSaslOAuthTokenProvider = "aws_msk_iam"
)

func (m *kafkaMetadata) Validate() error {
	if m.LagThreshold <= 0 {
		return fmt.Errorf("lagThreshold must be positive number")
	}
	if m.ActivationLagThreshold < 0 {
		return fmt.Errorf("activationLagThreshold must be positive number")
	}
	if m.AllowIdleConsumers && m.LimitToPartitionsWithLag {
		return fmt.Errorf("allowIdleConsumers and limitToPartitionsWithLag cannot be set simultaneously")
	}
	if m.LimitToPartitionsWithLag && m.EnsureEvenDistributionOfPartitions {
		return fmt.Errorf("limitToPartitionsWithLag and ensureEvenDistributionOfPartitions cannot be set simultaneously")
	}
	if m.Topic == "" {
		if m.LimitToPartitionsWithLag {
			return fmt.Errorf("topic must be specified when using limitToPartitionsWithLag")
		}
		if m.EnsureEvenDistributionOfPartitions {
			return fmt.Errorf("topic must be specified when using ensureEvenDistributionOfPartitions")
		}
	}
	m.offsetResetPolicy = offsetResetPolicy(m.OffsetResetPolicy)
	if err := m.parseTLS(); err != nil {
		return err
	}
	if err := m.parseSASL(); err != nil {
		return err
	}
	return nil
}

func (m *kafkaMetadata) parseTLS() error {
	m.enableTLS = false
	if m.TLS == stringEnable {
		m.enableTLS = true
		if (m.Cert != "") != (m.Key != "") {
			return errors.New("cert and key must both be provided or both be empty")
		}
	}
	return nil
}

func (m *kafkaMetadata) parseSASL() error {
	m.saslType = KafkaSASLTypeNone
	if m.SASL == "" || m.SASL == "none" {
		return nil
	}
	m.saslType = kafkaSaslType(m.SASL)
	switch m.saslType {
	case KafkaSASLTypePlaintext, KafkaSASLTypeSCRAMSHA256, KafkaSASLTypeSCRAMSHA512:
		return m.validateBasicSASL()
	case KafkaSASLTypeOAuthbearer:
		return m.validateOAuthSASL()
	case KafkaSASLTypeGSSAPI:
		return m.validateKerberosSASL()
	default:
		return fmt.Errorf("unsupported SASL mode: %s", m.saslType)
	}
}

func (m *kafkaMetadata) validateBasicSASL() error {
	if m.Username == "" {
		return errors.New("no username given")
	}
	if m.Password == "" {
		return errors.New("no password given")
	}
	return nil
}

func (m *kafkaMetadata) validateOAuthSASL() error {
	m.tokenProvider = KafkaSASLOAuthTokenProviderBearer
	if m.SAMLTokenProvider != "" {
		m.tokenProvider = kafkaSaslOAuthTokenProvider(m.SAMLTokenProvider)
	}
	switch m.tokenProvider {
	case KafkaSASLOAuthTokenProviderBearer:
		return m.validateOAuthBearer()
	case KafkaSASLOAuthTokenProviderAWSMSKIAM:
		return m.validateAWSMSKIAM()
	default:
		return fmt.Errorf("unsupported OAuth token provider: %s", m.tokenProvider)
	}
}

func (m *kafkaMetadata) validateOAuthBearer() error {
	if m.Username == "" {
		return errors.New("no username given")
	}
	if m.Password == "" {
		return errors.New("no password given")
	}
	if m.OAuthTokenEndpointURI == "" {
		return errors.New("no oauth token endpoint uri given")
	}
	if m.Scopes != "" {
		m.scopes = strings.Split(m.Scopes, ",")
	}
	m.oauthExtensions = make(map[string]string)
	if m.OAuthExtensions != "" {
		for _, ext := range strings.Split(m.OAuthExtensions, ",") {
			parts := strings.Split(ext, "=")
			if len(parts) != 2 {
				return errors.New("invalid OAuthBearer extension, must be of format key=value")
			}
			m.oauthExtensions[parts[0]] = parts[1]
		}
	}
	return nil
}

func (m *kafkaMetadata) validateAWSMSKIAM() error {
	if !m.enableTLS {
		return errors.New("TLS is required for AWS MSK authentication")
	}
	if m.AWSRegion == "" {
		return errors.New("no awsRegion given")
	}
	return nil
}

func (m *kafkaMetadata) validateKerberosSASL() error {
	if m.Username == "" {
		return errors.New("no username given")
	}
	if (m.Password == "" && m.Keytab == "") || (m.Password != "" && m.Keytab != "") {
		return errors.New("exactly one of 'password' or 'keytab' must be provided for GSSAPI authentication")
	}
	if m.Realm == "" {
		return errors.New("no realm given")
	}
	if m.KerberosConfig == "" {
		return errors.New("no Kerberos configuration file (kerberosConfig) given")
	}
	if m.Keytab != "" {
		path, err := saveToFile(m.Keytab)
		if err != nil {
			return fmt.Errorf("error saving keytab to file: %w", err)
		}
		m.keytabPath = path
	}
	path, err := saveToFile(m.KerberosConfig)
	if err != nil {
		return fmt.Errorf("error saving kerberosConfig to file: %w", err)
	}
	m.kerberosConfigPath = path
	return nil
}

func saveToFile(content string) (string, error) {
	tempKrbDir := fmt.Sprintf("%s%c%s", os.TempDir(), os.PathSeparator, "kerberos")
	if err := os.MkdirAll(tempKrbDir, 0700); err != nil {
		return "", fmt.Errorf("error creating temporary directory %s: %w", tempKrbDir, err)
	}
	tempFile, err := os.CreateTemp(tempKrbDir, "krb_*")
	if err != nil {
		return "", fmt.Errorf("error creating temporary file: %w", err)
	}
	defer tempFile.Close()
	if _, err := tempFile.Write([]byte(content)); err != nil {
		return "", fmt.Errorf("error writing to temporary file: %w", err)
	}
	return tempFile.Name(), nil
}

func NewKafkaScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}
	logger := InitializeLogger(config, "kafka_scaler")
	meta, err := parseKafkaMetadata(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing kafka metadata: %w", err)
	}
	client, admin, err := getKafkaClients(ctx, meta)
	if err != nil {
		return nil, err
	}
	return &kafkaScaler{
		client:          client,
		admin:           admin,
		metricType:      metricType,
		metadata:        meta,
		logger:          logger,
		previousOffsets: make(map[string]map[int32]int64),
	}, nil
}

func parseKafkaMetadata(config *scalersconfig.ScalerConfig, logger logr.Logger) (*kafkaMetadata, error) {
	meta := &kafkaMetadata{}
	if err := config.TypedConfig(meta); err != nil {
		return nil, err
	}
	version, err := sarama.ParseKafkaVersion(meta.Version)
	if err != nil {
		return nil, fmt.Errorf("error parsing kafka version: %w", err)
	}
	meta.version = version
	if meta.saslType == KafkaSASLTypeOAuthbearer && meta.tokenProvider == KafkaSASLOAuthTokenProviderAWSMSKIAM {
		auth, err := awsutils.GetAwsAuthorization(config.TriggerUniqueKey, meta.AWSRegion, config.PodIdentity, config.TriggerMetadata, config.AuthParams, config.ResolvedEnv)
		if err != nil {
			return nil, fmt.Errorf("error getting AWS authorization: %w", err)
		}
		meta.awsAuthorization = auth
	}
	meta.triggerIndex = config.TriggerIndex
	if meta.Topic == "" {
		logger.V(1).Info(fmt.Sprintf("consumer group %q has no topic specified, will use all topics subscribed by the consumer group for scaling", meta.Group))
		if len(meta.PartitionLimitation) > 0 {
			logger.V(1).Info("no specific topic set, ignoring partitionLimitation setting")
			meta.PartitionLimitation = nil
		}
	} else if len(meta.PartitionLimitation) > 0 {
		logger.V(0).Info(fmt.Sprintf("partition limit active '%v'", meta.PartitionLimitation))
	}
	return meta, nil
}

func getKafkaClients(ctx context.Context, metadata *kafkaMetadata) (sarama.Client, sarama.ClusterAdmin, error) {
	config, err := getKafkaClientConfig(ctx, metadata)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting kafka client config: %w", err)
	}
	client, err := sarama.NewClient(metadata.BootstrapServers, config)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating kafka client: %w", err)
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		if !client.Closed() {
			client.Close()
		}
		return nil, nil, fmt.Errorf("error creating kafka admin: %w", err)
	}
	return client, admin, nil
}

func getKafkaClientConfig(ctx context.Context, meta *kafkaMetadata) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = meta.version
	if meta.saslType != KafkaSASLTypeNone && meta.saslType != KafkaSASLTypeGSSAPI {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = meta.Username
		config.Net.SASL.Password = meta.Password
	}
	if meta.enableTLS {
		config.Net.TLS.Enable = true
		tlsConfig, err := kedautil.NewTLSConfigWithPassword(meta.Cert, meta.Key, meta.KeyPassword, meta.CA, meta.UnsafeSsl)
		if err != nil {
			return nil, err
		}
		config.Net.TLS.Config = tlsConfig
	}
	if meta.saslType == KafkaSASLTypePlaintext {
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}
	if meta.saslType == KafkaSASLTypeSCRAMSHA256 {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &kafka.XDGSCRAMClient{HashGeneratorFcn: kafka.SHA256} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	}
	if meta.saslType == KafkaSASLTypeSCRAMSHA512 {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &kafka.XDGSCRAMClient{HashGeneratorFcn: kafka.SHA512} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	}
	if meta.saslType == KafkaSASLTypeOAuthbearer {
		config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		switch meta.tokenProvider {
		case KafkaSASLOAuthTokenProviderBearer:
			config.Net.SASL.TokenProvider = kafka.OAuthBearerTokenProvider(meta.Username, meta.Password, meta.OAuthTokenEndpointURI, meta.scopes, meta.oauthExtensions)
		case KafkaSASLOAuthTokenProviderAWSMSKIAM:
			awsAuth, err := awsutils.GetAwsConfig(ctx, meta.awsAuthorization)
			if err != nil {
				return nil, fmt.Errorf("error getting AWS config: %w", err)
			}
			config.Net.SASL.TokenProvider = kafka.OAuthMSKTokenProvider(awsAuth)
		default:
			return nil, fmt.Errorf("unsupported SASL OAuth token provider: %s", meta.tokenProvider)
		}
	}
	if meta.saslType == KafkaSASLTypeGSSAPI {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		if meta.KerberosServiceName != "" {
			config.Net.SASL.GSSAPI.ServiceName = meta.KerberosServiceName
		} else {
			config.Net.SASL.GSSAPI.ServiceName = "kafka"
		}
		config.Net.SASL.GSSAPI.Username = meta.Username
		config.Net.SASL.GSSAPI.Realm = meta.Realm
		config.Net.SASL.GSSAPI.KerberosConfigPath = meta.kerberosConfigPath
		if meta.keytabPath != "" {
			config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
			config.Net.SASL.GSSAPI.KeyTabPath = meta.keytabPath
		} else {
			config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
			config.Net.SASL.GSSAPI.Password = meta.Password
		}
		if meta.KerberosDisableFAST {
			config.Net.SASL.GSSAPI.DisablePAFXFAST = true
		}
	}
	return config, nil
}

func (s *kafkaScaler) getTopicPartitions() (map[string][]int32, error) {
	var topicsToDescribe []string
	if s.metadata.Topic == "" {
		resp, err := s.admin.ListConsumerGroupOffsets(s.metadata.Group, nil)
		if err != nil {
			return nil, fmt.Errorf("error listing cg offset: %w", err)
		}
		if resp.Err > 0 {
			return nil, fmt.Errorf("error listing cg offset: %w", resp.Err)
		}
		for topic := range resp.Blocks {
			topicsToDescribe = append(topicsToDescribe, topic)
		}
	} else {
		topicsToDescribe = []string{s.metadata.Topic}
	}
	topicsMetadata, err := s.admin.DescribeTopics(topicsToDescribe)
	if err != nil {
		return nil, fmt.Errorf("error describing topics: %w", err)
	}
	s.logger.V(1).Info(fmt.Sprintf("topic %s metadata: %v", topicsToDescribe, topicsMetadata))
	if s.metadata.Topic != "" && len(topicsMetadata) != 1 {
		return nil, fmt.Errorf("expected 1 topic metadata, got %d", len(topicsMetadata))
	}
	result := make(map[string][]int32, len(topicsMetadata))
	for _, tm := range topicsMetadata {
		if tm.Err > 0 {
			return nil, fmt.Errorf("error describing topics: %w", tm.Err)
		}
		var partitions []int32
		for _, p := range tm.Partitions {
			if s.isActivePartition(p.ID) {
				partitions = append(partitions, p.ID)
			}
		}
		if len(partitions) == 0 {
			return nil, fmt.Errorf("no active partitions in topic %s", tm.Name)
		}
		result[tm.Name] = partitions
	}
	return result, nil
}

func (s *kafkaScaler) isActivePartition(pID int32) bool {
	if s.metadata.PartitionLimitation == nil {
		return true
	}
	for _, id := range s.metadata.PartitionLimitation {
		if pID == id {
			return true
		}
	}
	return false
}

func (s *kafkaScaler) getConsumerOffsets(topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	offsets, err := s.admin.ListConsumerGroupOffsets(s.metadata.Group, topicPartitions)
	if err != nil {
		return nil, fmt.Errorf("error listing consumer group offsets: %w", err)
	}
	if offsets.Err > 0 {
		return nil, fmt.Errorf("error listing consumer group offsets: %w", offsets.Err)
	}
	return offsets, nil
}

func (s *kafkaScaler) getLagForPartition(topic string, partID int32, offsets *sarama.OffsetFetchResponse, topicOffsets map[string]map[int32]int64) (int64, int64, error) {
	block := offsets.GetBlock(topic, partID)
	if block == nil {
		return 0, 0, fmt.Errorf("no offset block for topic %s partition %d", topic, partID)
	}
	if block.Err > 0 {
		return 0, 0, fmt.Errorf("offset block error for topic %s partition %d: %w", topic, partID, block.Err)
	}
	consumerOffset := block.Offset
	if consumerOffset == invalidOffset && s.metadata.offsetResetPolicy == latest {
		retVal := int64(1)
		if s.metadata.ScaleToZeroOnInvalidOffset {
			retVal = 0
		}
		s.logger.V(1).Info(fmt.Sprintf("invalid offset for topic %s group %s partition %d, returning lag %d", topic, s.metadata.Group, partID, retVal))
		return retVal, retVal, nil
	}
	if _, found := topicOffsets[topic]; !found {
		return 0, 0, fmt.Errorf("no partition offset for topic %s", topic)
	}
	latestOffset := topicOffsets[topic][partID]
	if consumerOffset == invalidOffset && s.metadata.offsetResetPolicy == earliest {
		if s.metadata.ScaleToZeroOnInvalidOffset {
			return 0, 0, nil
		}
		return latestOffset, latestOffset, nil
	}
	if s.metadata.ExcludePersistentLag {
		if prevOffset, found := s.previousOffsets[topic][partID]; !found {
			if _, ok := s.previousOffsets[topic]; !ok {
				s.previousOffsets[topic] = map[int32]int64{partID: consumerOffset}
			} else {
				s.previousOffsets[topic][partID] = consumerOffset
			}
		} else if prevOffset == consumerOffset {
			return 0, latestOffset - consumerOffset, nil
		} else {
			s.previousOffsets[topic][partID] = consumerOffset
		}
	}
	return latestOffset - consumerOffset, latestOffset - consumerOffset, nil
}

func (s *kafkaScaler) Close(context.Context) error {
	if s.metadata.kerberosConfigPath != "" {
		os.Remove(s.metadata.kerberosConfigPath)
	}
	if s.metadata.keytabPath != "" {
		os.Remove(s.metadata.keytabPath)
	}
	if s.admin != nil {
		return s.admin.Close()
	}
	return nil
}

func (s *kafkaScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	name := fmt.Sprintf("kafka-%s", s.metadata.Topic)
	if s.metadata.Topic == "" {
		name = fmt.Sprintf("kafka-%s-topics", s.metadata.Group)
	}
	return []v2.MetricSpec{{
		External: &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{
				Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, kedautil.NormalizeString(name)),
			},
			Target: GetMetricTarget(s.metricType, s.metadata.LagThreshold),
		},
		Type: "External",
	}}
}

type consumerOffsetResult struct {
	consumerOffsets *sarama.OffsetFetchResponse
	err             error
}

type producerOffsetResult struct {
	producerOffsets map[string]map[int32]int64
	err             error
}

func (s *kafkaScaler) getConsumerAndProducerOffsets(topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, map[string]map[int32]int64, error) {
	consumerChan := make(chan consumerOffsetResult, 1)
	go func() {
		offsets, err := s.getConsumerOffsets(topicPartitions)
		consumerChan <- consumerOffsetResult{offsets, err}
	}()
	producerChan := make(chan producerOffsetResult, 1)
	go func() {
		offsets, err := s.getProducerOffsets(topicPartitions)
		producerChan <- producerOffsetResult{offsets, err}
	}()
	consumerRes := <-consumerChan
	if consumerRes.err != nil {
		return nil, nil, consumerRes.err
	}
	producerRes := <-producerChan
	if producerRes.err != nil {
		return nil, nil, producerRes.err
	}
	return consumerRes.consumerOffsets, producerRes.producerOffsets, nil
}

func (s *kafkaScaler) GetMetricsAndActivity(_ context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	totalLag, totalLagWithPersistent, err := s.getTotalLag()
	if err != nil {
		return nil, false, err
	}
	return []external_metrics.ExternalMetricValue{GenerateMetricInMili(metricName, float64(totalLag))}, totalLagWithPersistent > s.metadata.ActivationLagThreshold, nil
}

func (s *kafkaScaler) getTotalLag() (int64, int64, error) {
	topicPartitions, err := s.getTopicPartitions()
	if err != nil {
		return 0, 0, err
	}
	consumerOffsets, producerOffsets, err := s.getConsumerAndProducerOffsets(topicPartitions)

	if err != nil {
		return 0, 0, err
	}
	var totalLag, totalLagWithPersistent, totalPartitions, partitionsWithLag int64
	for topic, partOffsets := range producerOffsets {
		for partition := range partOffsets {
			lag, lagWithPersistent, err := s.getLagForPartition(topic, partition, consumerOffsets, producerOffsets)
			if err != nil {
				return 0, 0, err
			}
			totalLag += lag
			totalLagWithPersistent += lagWithPersistent
			if lag > 0 {
				partitionsWithLag++
			}
		}
		totalPartitions += int64(len(partOffsets))
	}
	s.logger.V(1).Info(fmt.Sprintf("Kafka scaler: totalLag=%v, topicPartitions=%v, threshold=%v", totalLag, len(topicPartitions), s.metadata.LagThreshold))
	if !s.metadata.AllowIdleConsumers || s.metadata.LimitToPartitionsWithLag || s.metadata.EnsureEvenDistributionOfPartitions {
		upperBound := totalPartitions
		if s.metadata.EnsureEvenDistributionOfPartitions {
			nextFactor := getNextFactorThatBalancesConsumersToTopicPartitions(totalLag, totalPartitions, s.metadata.LagThreshold)
			s.logger.V(1).Info(fmt.Sprintf("Kafka scaler: ensuring even distribution totalLag=%v, partitions=%v, evenPartitions=%v", totalLag, totalPartitions, nextFactor))
			totalLag = nextFactor * s.metadata.LagThreshold
		}
		if s.metadata.LimitToPartitionsWithLag {
			upperBound = partitionsWithLag
		}
		if (totalLag / s.metadata.LagThreshold) > upperBound {
			totalLag = upperBound * s.metadata.LagThreshold
		}
	}
	return totalLag, totalLagWithPersistent, nil
}

func getNextFactorThatBalancesConsumersToTopicPartitions(totalLag, totalPartitions, lagThreshold int64) int64 {
	factors := FindFactors(totalPartitions)
	for _, factor := range factors {
		if factor*lagThreshold >= totalLag {
			return factor
		}
	}
	return totalPartitions
}

type brokerOffsetResult struct {
	offsetResp *sarama.OffsetResponse
	err        error
}

func (s *kafkaScaler) getProducerOffsets(topicPartitions map[string][]int32) (map[string]map[int32]int64, error) {
	version := int16(0)
	if s.client.Config().Version.IsAtLeast(sarama.V0_10_1_0) {
		version = 1
	}
	requests := make(map[*sarama.Broker]*sarama.OffsetRequest)
	for topic, partitions := range topicPartitions {
		for _, partID := range partitions {
			broker, err := s.client.Leader(topic, partID)
			if err != nil {
				return nil, err
			}
			request, ok := requests[broker]
			if !ok {
				request = &sarama.OffsetRequest{Version: version}
				requests[broker] = request
			}
			request.AddBlock(topic, partID, sarama.OffsetNewest, 1)
		}
	}
	resultCh := make(chan brokerOffsetResult, len(requests))
	var wg sync.WaitGroup
	wg.Add(len(requests))
	for broker, request := range requests {
		go func(br *sarama.Broker, req *sarama.OffsetRequest) {
			defer wg.Done()
			response, err := br.GetAvailableOffsets(req)
			resultCh <- brokerOffsetResult{response, err}
		}(broker, request)
	}
	wg.Wait()
	close(resultCh)
	topicPartOffsets := make(map[string]map[int32]int64)
	for res := range resultCh {
		if res.err != nil {
			return nil, res.err
		}
		for topic, blocks := range res.offsetResp.Blocks {
			if _, found := topicPartOffsets[topic]; !found {
				topicPartOffsets[topic] = make(map[int32]int64)
			}
			for partID, block := range blocks {
				if block.Err != sarama.ErrNoError {
					return nil, block.Err
				}
				topicPartOffsets[topic][partID] = block.Offset
			}
		}
	}
	return topicPartOffsets, nil
}

func FindFactors(n int64) []int64 {
	if n < 1 {
		return nil
	}
	var factors []int64
	sqrtN := int64(math.Sqrt(float64(n)))
	for i := int64(1); i <= sqrtN; i++ {
		if n%i == 0 {
			factors = append(factors, i)
			if i != n/i {
				factors = append(factors, n/i)
			}
		}
	}
	sort.Slice(factors, func(i, j int) bool {
		return factors[i] < factors[j]
	})
	return factors
}
