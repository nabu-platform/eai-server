package be.nabu.eai.server.rest;

public enum ProjectType {
	// a utility project that is meant to support other projects
	// it can contain frameworks, reusable technical bits
	// the nabu package is a typical utility project
	UTILITY,
	// an integration project
	INTEGRATION,
	// a business application focuses on business logic, branding,...
	// it can contain workflows, data models, api's,...
	// a typical example is a microservice which encapsulates business logic
	BUSINESS,
	// the default type: a (web) application
	APPLICATION,
	// a test project
	TESTING
}
