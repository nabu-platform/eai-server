The repository contains `projects` at its root. Each project has its own hierarchic namespace that contains artifacts.
Both the namespace and artifact names must be be valid java variable names in camelCase.

A project can be one of these types:

- `integration`: all logic pertaining to a certain integration is kept here. integration projects should rarely depend on one another.
- `business`: all logic pertaining to a business domain is kept here, one project per non overlapping domain. business logic can use other business projects and integration projects
- `application`: usually a web application which bundles together one or more domains and exposes both the API and frontend for this, can depend on business projects and integrations, rarely on other applications.
- `utility`: a project that contains only reusable services and types
- `testing`: a project that contains test artifacts

For integrations we use this namespace layout:

- <project>.integration.in: contains all APIs we expose to said system (e.g. webhooks). If webhooks need business domain logic, we put only the spec of an API here and implement in the business logic
- <project>.integration.out: contains all artifacts we use to connect to the target system, for example WSDL client, swagger client, rest clients, odata,...

If we use multiple swaggers or multiple rest clients, we centralize shared configuration in an endpoint and reuse that cross artifact.

Services that are reusable by other projects should go into <project>.services namespace (sub namespaces can be added for clarity, for example <project>.services.contract)
Services that are narrowly reusable within the own project but should not be used by other projects go into <project>.utils

For non integration projects:
- Public REST provider services are added to <project>.api.rest
- Internal REST Provider services are added to <project>.manage.rest

We typically add a component at the root of every subfolder that contains all the rest services within it, for example <project>.api.rest.contract.component would contain <project>.api.rest.contract.list and <project>.api.rest.contract.create and...

Service specifications (contracts without implementation) are added to <project>.specs

Canonical structures can be added to <project>.types, localized structure helpers are defined next to the artifact that needs them.

Configuration definition and instance are added to <project>.configuration as <project>.configuration.definition (a structure) and <project>.configuration.instance

Database connections are named <project>.databases.<name>.connection where name is a logical name that contains everything related to that database.
Structures representing the database tables are added in <project>.databases.<name>.types
A datamodel to view these structures is added as <project>.databases.<name>.model
We always create structures first, then generate the tables from there.

Rules:
- Avoid circular dependency chains between projects.
- Prefer small, composable artifacts over large multi-purpose ones. Large artifacts are harder to review, reuse, test, and evolve safely.
- Be mindful of backwards compatibility when updating existing artifacts. notify the user if something will break.
- Avoid using deprecated artifacts