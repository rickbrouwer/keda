// Code generated by smithy-go-codegen DO NOT EDIT.

package amp

import (
	"context"
	"fmt"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// The CreateScraper operation creates a scraper to collect metrics. A scraper
// pulls metrics from Prometheus-compatible sources within an Amazon EKS cluster,
// and sends them to your Amazon Managed Service for Prometheus workspace. Scrapers
// are flexible, and can be configured to control what metrics are collected, the
// frequency of collection, what transformations are applied to the metrics, and
// more.
//
// An IAM role will be created for you that Amazon Managed Service for Prometheus
// uses to access the metrics in your cluster. You must configure this role with a
// policy that allows it to scrape metrics from your cluster. For more information,
// see [Configuring your Amazon EKS cluster]in the Amazon Managed Service for Prometheus User Guide.
//
// The scrapeConfiguration parameter contains the base-64 encoded YAML
// configuration for the scraper.
//
// For more information about collectors, including what metrics are collected,
// and how to configure the scraper, see [Using an Amazon Web Services managed collector]in the Amazon Managed Service for
// Prometheus User Guide.
//
// [Using an Amazon Web Services managed collector]: https://docs.aws.amazon.com/prometheus/latest/userguide/AMP-collector-how-to.html
// [Configuring your Amazon EKS cluster]: https://docs.aws.amazon.com/prometheus/latest/userguide/AMP-collector-how-to.html#AMP-collector-eks-setup
func (c *Client) CreateScraper(ctx context.Context, params *CreateScraperInput, optFns ...func(*Options)) (*CreateScraperOutput, error) {
	if params == nil {
		params = &CreateScraperInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "CreateScraper", params, optFns, c.addOperationCreateScraperMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*CreateScraperOutput)
	out.ResultMetadata = metadata
	return out, nil
}

// Represents the input of a CreateScraper operation.
type CreateScraperInput struct {

	// The Amazon Managed Service for Prometheus workspace to send metrics to.
	//
	// This member is required.
	Destination types.Destination

	// The configuration file to use in the new scraper. For more information, see [Scraper configuration] in
	// the Amazon Managed Service for Prometheus User Guide.
	//
	// [Scraper configuration]: https://docs.aws.amazon.com/prometheus/latest/userguide/AMP-collector-how-to.html#AMP-collector-configuration
	//
	// This member is required.
	ScrapeConfiguration types.ScrapeConfiguration

	// The Amazon EKS cluster from which the scraper will collect metrics.
	//
	// This member is required.
	Source types.Source

	// (optional) An alias to associate with the scraper. This is for your use, and
	// does not need to be unique.
	Alias *string

	// (Optional) A unique, case-sensitive identifier that you can provide to ensure
	// the idempotency of the request.
	ClientToken *string

	// The scraper role configuration for the workspace.
	RoleConfiguration *types.RoleConfiguration

	// (Optional) The list of tag keys and values to associate with the scraper.
	Tags map[string]string

	noSmithyDocumentSerde
}

// Represents the output of a CreateScraper operation.
type CreateScraperOutput struct {

	// The Amazon Resource Name (ARN) of the new scraper.
	//
	// This member is required.
	Arn *string

	// The ID of the new scraper.
	//
	// This member is required.
	ScraperId *string

	// A structure that displays the current status of the scraper.
	//
	// This member is required.
	Status *types.ScraperStatus

	// The list of tag keys and values that are associated with the scraper.
	Tags map[string]string

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationCreateScraperMiddlewares(stack *middleware.Stack, options Options) (err error) {
	if err := stack.Serialize.Add(&setOperationInputMiddleware{}, middleware.After); err != nil {
		return err
	}
	err = stack.Serialize.Add(&awsRestjson1_serializeOpCreateScraper{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsRestjson1_deserializeOpCreateScraper{}, middleware.After)
	if err != nil {
		return err
	}
	if err := addProtocolFinalizerMiddlewares(stack, options, "CreateScraper"); err != nil {
		return fmt.Errorf("add protocol finalizers: %v", err)
	}

	if err = addlegacyEndpointContextSetter(stack, options); err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = addClientRequestID(stack); err != nil {
		return err
	}
	if err = addComputeContentLength(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = addComputePayloadSHA256(stack); err != nil {
		return err
	}
	if err = addRetry(stack, options); err != nil {
		return err
	}
	if err = addRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = addRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addSpanRetryLoop(stack, options); err != nil {
		return err
	}
	if err = addClientUserAgent(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addSetLegacyContextSigningOptionsMiddleware(stack); err != nil {
		return err
	}
	if err = addTimeOffsetBuild(stack, c); err != nil {
		return err
	}
	if err = addUserAgentRetryMode(stack, options); err != nil {
		return err
	}
	if err = addCredentialSource(stack, options); err != nil {
		return err
	}
	if err = addIdempotencyToken_opCreateScraperMiddleware(stack, options); err != nil {
		return err
	}
	if err = addOpCreateScraperValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opCreateScraper(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addRecursionDetection(stack); err != nil {
		return err
	}
	if err = addRequestIDRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = addDisableHTTPSMiddleware(stack, options); err != nil {
		return err
	}
	if err = addSpanInitializeStart(stack); err != nil {
		return err
	}
	if err = addSpanInitializeEnd(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestStart(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestEnd(stack); err != nil {
		return err
	}
	return nil
}

type idempotencyToken_initializeOpCreateScraper struct {
	tokenProvider IdempotencyTokenProvider
}

func (*idempotencyToken_initializeOpCreateScraper) ID() string {
	return "OperationIdempotencyTokenAutoFill"
}

func (m *idempotencyToken_initializeOpCreateScraper) HandleInitialize(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	if m.tokenProvider == nil {
		return next.HandleInitialize(ctx, in)
	}

	input, ok := in.Parameters.(*CreateScraperInput)
	if !ok {
		return out, metadata, fmt.Errorf("expected middleware input to be of type *CreateScraperInput ")
	}

	if input.ClientToken == nil {
		t, err := m.tokenProvider.GetIdempotencyToken()
		if err != nil {
			return out, metadata, err
		}
		input.ClientToken = &t
	}
	return next.HandleInitialize(ctx, in)
}
func addIdempotencyToken_opCreateScraperMiddleware(stack *middleware.Stack, cfg Options) error {
	return stack.Initialize.Add(&idempotencyToken_initializeOpCreateScraper{tokenProvider: cfg.IdempotencyTokenProvider}, middleware.Before)
}

func newServiceMetadataMiddleware_opCreateScraper(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		OperationName: "CreateScraper",
	}
}
