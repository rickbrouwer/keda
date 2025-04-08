// Code generated by tutone: DO NOT EDIT
package installevents

import (
	"github.com/newrelic/newrelic-client-go/v2/pkg/common"
	"github.com/newrelic/newrelic-client-go/v2/pkg/nrtime"
)

// InstallationInstallStateType - An enum that represent the installation state.
type InstallationInstallStateType string

var InstallationInstallStateTypeTypes = struct {
	// Defines a completed installation.
	COMPLETED InstallationInstallStateType
	// Defines an installation that has been started.
	STARTED InstallationInstallStateType
}{
	// Defines a completed installation.
	COMPLETED: "COMPLETED",
	// Defines an installation that has been started.
	STARTED: "STARTED",
}

// InstallationRecipeStatusType - An enum that represents the various recipe statuses.
type InstallationRecipeStatusType string

var InstallationRecipeStatusTypeTypes = struct {
	// Defines an available recipe when attempting to install.
	AVAILABLE InstallationRecipeStatusType
	// Defines a canceled recipe when attempting to install.
	CANCELED InstallationRecipeStatusType
	// Defines when New Relic instrumentation compatibility is detected.
	DETECTED InstallationRecipeStatusType
	// Defines a recipe that has failed during installation.
	FAILED InstallationRecipeStatusType
	// Defines a recipe that has been installed.
	INSTALLED InstallationRecipeStatusType
	// Defines a recipe currently being installed.
	INSTALLING InstallationRecipeStatusType
	// Defines a recipe that has been recommended during installation.
	RECOMMENDED InstallationRecipeStatusType
	// Defines a recipe that has been skipped during installation.
	SKIPPED InstallationRecipeStatusType
	// Defines a recipe that is unsupported.
	UNSUPPORTED InstallationRecipeStatusType
}{
	// Defines an available recipe when attempting to install.
	AVAILABLE: "AVAILABLE",
	// Defines a canceled recipe when attempting to install.
	CANCELED: "CANCELED",
	// Defines when New Relic instrumentation compatibility is detected.
	DETECTED: "DETECTED",
	// Defines a recipe that has failed during installation.
	FAILED: "FAILED",
	// Defines a recipe that has been installed.
	INSTALLED: "INSTALLED",
	// Defines a recipe currently being installed.
	INSTALLING: "INSTALLING",
	// Defines a recipe that has been recommended during installation.
	RECOMMENDED: "RECOMMENDED",
	// Defines a recipe that has been skipped during installation.
	SKIPPED: "SKIPPED",
	// Defines a recipe that is unsupported.
	UNSUPPORTED: "UNSUPPORTED",
}

// InstallationInstallStatus - An object that contains the overall installation status that is created from within the newrelic-cli.
type InstallationInstallStatus struct {
	// The version of the newrelic-cli that was used for a given installation attempt.
	CliVersion string `json:"cliVersion"`
	// Refers to the source of the installation.
	DeployedBy string `json:"deployedBy"`
	// Whether or not the installation is using a proxy.
	EnabledProxy bool `json:"enabledProxy"`
	// The error returned for a given installation attempt.
	Error InstallationStatusError `json:"error"`
	// The host name of the customer's machine.
	HostName string `json:"hostName"`
	// The unique ID that corresponds to an install status.
	InstallId string `json:"installId"`
	// The version of the open-install-library that is being used.
	InstallLibraryVersion string `json:"installLibraryVersion"`
	// Whether or not the installation is supported on the host machine.
	IsUnsupported bool `json:"isUnsupported"`
	// The kernel architecture of the customer's machine.
	KernelArch string `json:"kernelArch"`
	// The kernel version of the customer's machine.
	KernelVersion string `json:"kernelVersion"`
	// The path to the log file on the customer's host.
	LogFilePath string `json:"logFilePath"`
	// The OS of the customer's machine.
	Os string `json:"os"`
	// The platform name provided by the open-install-library.
	Platform string `json:"platform"`
	// The platform family name provided by the open-install-library.
	PlatformFamily string `json:"platformFamily"`
	// The platform version provided by the open-install-library.
	PlatformVersion string `json:"platformVersion"`
	// A URL generated by the newrelic-cli that redirects to the appropriate entity once an installation is complete.
	RedirectURL string `json:"redirectUrl"`
	// The state of the installation.
	State InstallationInstallStateType `json:"state"`
	// Whether or not the installation is a targeted install.
	TargetedInstall bool `json:"targetedInstall"`
	// The timestamp for when the install event occurred.
	Timestamp nrtime.EpochSeconds `json:"timestamp,omitempty"`
}

// InstallationInstallStatusInput - An object that contains the overall installation status to be created.
type InstallationInstallStatusInput struct {
	// The version of the newrelic-cli that was used for a given installation attempt.
	CliVersion string `json:"cliVersion"`
	// Refers to the source of the installation.
	DeployedBy string `json:"deployedBy,omitempty"`
	// Whether or not the installation is using a proxy.
	EnabledProxy bool `json:"enabledProxy"`
	// The error for a given installation attempt.
	Error InstallationStatusErrorInput `json:"error,omitempty"`
	// The host name of the customer's machine.
	HostName string `json:"hostName"`
	// The unique ID that corresponds to an install status.
	InstallId string `json:"installId"`
	// The version of the open-install-library that is being used.
	InstallLibraryVersion string `json:"installLibraryVersion"`
	// Whether or not the installation is supported on the host machine.
	IsUnsupported bool `json:"isUnsupported"`
	// The kernel architecture of the customer's machine.
	KernelArch string `json:"kernelArch"`
	// The kernel version of the customer's machine.
	KernelVersion string `json:"kernelVersion"`
	// The path to the log file on the customer's host.
	LogFilePath string `json:"logFilePath"`
	// The OS of the customer's machine.
	Os string `json:"os"`
	// The platform name provided by the open-install-library.
	Platform string `json:"platform"`
	// The platform family name provided by the open-install-library.
	PlatformFamily string `json:"platformFamily"`
	// The platform version provided by the open-install-library.
	PlatformVersion string `json:"platformVersion"`
	// A URL generated by the newrelic-cli that redirects to the appropriate entity once an installation is complete.
	RedirectURL string `json:"redirectUrl"`
	// The state of the installation.
	State InstallationInstallStateType `json:"state"`
	// Whether or not the installation is a targeted install.
	TargetedInstall bool `json:"targetedInstall"`
	// The timestamp for when the install event occurred.
	Timestamp nrtime.EpochSeconds `json:"timestamp,omitempty"`
}

// InstallationRecipeEvent - An object that contains an installation event created from within the newrelic-cli.
type InstallationRecipeEvent struct {
	// The version of the newrelic-cli that was used for a given recipe.
	CliVersion string `json:"cliVersion"`
	// Whether or not the recipe has been installed and all steps have been completed.
	Complete bool `json:"complete"`
	// The display name for a given recipe.
	DisplayName string `json:"displayName"`
	// The entity Guid for a given recipe.
	EntityGUID common.EntityGUID `json:"entityGuid"`
	// The error returned for a given recipe.
	Error InstallationStatusError `json:"error"`
	// The host name of the customer's machine.
	HostName string `json:"hostName"`
	// The unique ID that corresponds to an install event.
	InstallId string `json:"installId,omitempty"`
	// The version of the open-install-library that is being used.
	InstallLibraryVersion string `json:"installLibraryVersion,omitempty"`
	// The kernel architecture of the customer's machine.
	KernelArch string `json:"kernelArch"`
	// The kernel version of the customer's machine.
	KernelVersion string `json:"kernelVersion"`
	// The path to the log file on the customer's host.
	LogFilePath string `json:"logFilePath"`
	// Additional key:value data related to the environment where the installation occurred.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	// The unique name for a given recipe.
	Name string `json:"name"`
	// The OS of the customer's machine.
	Os string `json:"os"`
	// The platform name provided by the open-install-library.
	Platform string `json:"platform"`
	// The platform family name provided by the open-install-library.
	PlatformFamily string `json:"platformFamily"`
	// The platform version provided by the open-install-library.
	PlatformVersion string `json:"platformVersion"`
	// The redirect URL created by the CLI used for redirecting to a particular entity.
	RedirectURL string `json:"redirectUrl,omitempty"`
	// The status for a given recipe.
	Status InstallationRecipeStatusType `json:"status"`
	// Whether or not the recipe being installed is a targeted install.
	TargetedInstall bool `json:"targetedInstall"`
	// The path to the installation task as defined in the open-install-library.
	TaskPath string `json:"taskPath,omitempty"`
	// The timestamp for when the recipe event occurred.
	Timestamp nrtime.EpochSeconds `json:"timestamp"`
	// The number of milliseconds it took to validate the recipe.
	ValidationDurationMilliseconds int64 `json:"validationDurationMilliseconds"`
}

// InstallationRecipeStatus - An object that represents a recipe status.
type InstallationRecipeStatus struct {
	// The version of the newrelic-cli that was used for a given recipe.
	CliVersion string `json:"cliVersion"`
	// Whether or not the recipe has been installed and all steps have been completed.
	Complete bool `json:"complete"`
	// The display name for a given recipe.
	DisplayName string `json:"displayName"`
	// The entity Guid for a given recipe.
	EntityGUID common.EntityGUID `json:"entityGuid"`
	// The error returned for a given recipe.
	Error InstallationStatusErrorInput `json:"error,omitempty"`
	// The host name of the customer's machine.
	HostName string `json:"hostName"`
	// The unique ID that corresponds to an install event.
	InstallId string `json:"installId,omitempty"`
	// The version of the open-install-library that is being used.
	InstallLibraryVersion string `json:"installLibraryVersion,omitempty"`
	// The kernel architecture of the customer's machine.
	KernelArch string `json:"kernelArch"`
	// The kernel version of the customer's machine.
	KernelVersion string `json:"kernelVersion"`
	// The path to the log file on the customer's host.
	LogFilePath string `json:"logFilePath"`
	// Additional key:value data related to an error or related to the environment where the installation occurred.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	// The unique name for a given recipe.
	Name string `json:"name"`
	// The OS of the customer's machine.
	Os string `json:"os"`
	// The platform name provided by the open-install-library.
	Platform string `json:"platform"`
	// The platform family name provided by the open-install-library.
	PlatformFamily string `json:"platformFamily"`
	// The platform version provided by the open-install-library.
	PlatformVersion string `json:"platformVersion"`
	// The redirect URL created by the CLI used for redirecting to a particular entity.
	RedirectURL string `json:"redirectUrl,omitempty"`
	// The status for a given recipe.
	Status InstallationRecipeStatusType `json:"status"`
	// Whether or not the recipe being installed is a targeted install.
	TargetedInstall bool `json:"targetedInstall"`
	// The path to the installation task as defined in the open-install-library.
	TaskPath string `json:"taskPath,omitempty"`
	// The number of milliseconds it took to validate the recipe.
	ValidationDurationMilliseconds int64 `json:"validationDurationMilliseconds"`
}

// InstallationStatusError - An object that represents a status error whenever an recipe has failed to install.
type InstallationStatusError struct {
	// Error details, if any.
	Details string `json:"details,omitempty"`
	// The actual error message.
	Message string `json:"message,omitempty"`
	// An optimised message for the error.
	OptimizedMessage string `json:"optimizedMessage,omitempty"`
}

// InstallationStatusErrorInput - An object that represents a status error whenever an recipe has failed to install.
type InstallationStatusErrorInput struct {
	// Error details, if any.
	Details string `json:"details,omitempty"`
	// The actual error message.
	Message string `json:"message,omitempty"`
	// An optimised message for the error.
	OptimizedMessage string `json:"optimizedMessage,omitempty"`
}
