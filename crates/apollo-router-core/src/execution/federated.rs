use crate::*;
use derivative::Derivative;
use futures::lock::Mutex;
use futures::prelude::*;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tracing::Instrument;
use tracing_futures::WithSubscriber;

/// Recursively validate a query plan node making sure that all services are known before we go
/// for execution.
///
/// This simplifies processing later as we can always guarantee that services are configured for
/// the plan.
///
/// # Arguments
///
///  *   `plan`: The root query plan node to validate.
fn validate_services_against_plan(
    service_registry: Arc<dyn ServiceRegistry>,
    plan: &PlanNode,
) -> Vec<FetchError> {
    plan.service_usage()
        .collect::<HashSet<_>>()
        .into_iter()
        .filter(|service| !service_registry.has(service))
        .map(|service| FetchError::ValidationUnknownServiceError {
            service: service.to_string(),
        })
        .collect::<Vec<_>>()
}

/// Recursively validate a query plan node making sure that all variable usages are known before we
/// go for execution.
///
/// This simplifies processing later as we can always guarantee that the variable usages are
/// available for the plan.
///
/// # Arguments
///
///  *   `plan`: The root query plan node to validate.
fn validate_request_variables_against_plan(
    request: Arc<GraphQLRequest>,
    plan: &PlanNode,
) -> Vec<FetchError> {
    let required = plan.variable_usage().collect::<HashSet<_>>();
    let provided = request
        .variables
        .keys()
        .map(|x| x.as_str())
        .collect::<HashSet<_>>();
    required
        .difference(&provided)
        .map(|x| FetchError::ValidationMissingVariable {
            name: x.to_string(),
        })
        .collect::<Vec<_>>()
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct FederatedGraph {
    #[derivative(Debug = "ignore")]
    query_planner: Box<dyn QueryPlanner>,
    service_registry: Arc<dyn ServiceRegistry>,
}

impl FederatedGraph {
    /// Create a `FederatedGraph` instance used to execute a GraphQL query.
    pub fn new(
        query_planner: Box<dyn QueryPlanner>,
        service_registry: Arc<dyn ServiceRegistry>,
    ) -> Self {
        Self {
            query_planner,
            service_registry,
        }
    }
}

impl GraphQLFetcher for FederatedGraph {
    fn stream(&self, request: GraphQLRequest) -> GraphQLResponseStream {
        let span = tracing::info_span!("federated_query");
        let _guard = span.enter();

        log::trace!("Request received:\n{:#?}", request);

        let plan = match self.query_planner.get(
            request.query.to_owned(),
            request.operation_name.to_owned(),
            Default::default(),
        ) {
            Ok(QueryPlan { node: Some(root) }) => root,
            Ok(_) => return stream::empty().boxed(),
            Err(err) => return stream::iter(vec![FetchError::from(err).to_response(true)]).boxed(),
        };
        let service_registry = self.service_registry.clone();
        let request = Arc::new(request);

        let mut early_errors = Vec::new();

        for err in validate_services_against_plan(service_registry.clone(), &plan) {
            early_errors.push(err.to_graphql_error(None));
        }

        for err in validate_request_variables_against_plan(request.clone(), &plan) {
            early_errors.push(err.to_graphql_error(None));
        }

        // If we have any errors so far then let's abort the query
        // Planning/validation/variables are candidates to abort.
        if !early_errors.is_empty() {
            return stream::once(
                async move { GraphQLResponse::builder().errors(early_errors).build() },
            )
            .boxed();
        }

        stream::once(
            async move {
                let response = GraphQLResponse::builder().build();
                let root = Path::empty();

                execute(&response, &root, &plan, request, service_registry.clone()).await
            }
            .in_current_span()
            .with_current_subscriber(),
        )
        .boxed()
    }
}

fn execute<'a>(
    response: &'a GraphQLResponse,
    current_dir: &'a Path,
    plan: &'a PlanNode,
    request: Arc<GraphQLRequest>,
    service_registry: Arc<dyn ServiceRegistry>,
) -> Pin<Box<dyn Future<Output = GraphQLResponse> + Send + 'a>> {
    let span = tracing::info_span!("execution");
    let _guard = span.enter();

    Box::pin(async move {
        log::trace!("Executing plan:\n{:#?}", plan);

        match plan {
            PlanNode::Sequence { nodes } => {
                let mut result = response.clone();
                for node in nodes {
                    result.merge(
                        execute(
                            &result,
                            current_dir,
                            node,
                            request.clone(),
                            service_registry.clone(),
                        )
                        .await,
                    );
                }
                result
            }
            PlanNode::Parallel { nodes } => {
                let sub_results = future::join_all(nodes.iter().map(|plan| {
                    execute(
                        response,
                        current_dir,
                        plan,
                        request.clone(),
                        service_registry.clone(),
                    )
                }))
                .await;
                // TODO implementing FromIter would help
                let mut result = response.clone();
                for sub_response in sub_results {
                    result.merge(sub_response);
                }
                result
            }
            PlanNode::Fetch(info) => {
                let result = fetch_node(
                    response,
                    current_dir,
                    info,
                    request.clone(),
                    service_registry.clone(),
                )
                .await;

                debug_assert!(result.is_ok(), "Fetch error: {:?}", result);

                match result {
                    Ok(sub_response) => {
                        log::trace!(
                            "New data:\n{}",
                            serde_json::to_string_pretty(&response.data).unwrap(),
                        );
                        let mut result = response.clone();
                        result.merge(sub_response);
                        result
                    }
                    Err(err) => {
                        log::error!("Fetch error: {}", err);
                        let mut result = response.clone();
                        result
                            .errors
                            .push(err.to_graphql_error(Some(current_dir.to_owned())));
                        result
                    }
                }
            }
            PlanNode::Flatten(FlattenNode { path, node }) => {
                // this is the only command that actually changes the "current dir"
                let current_dir = current_dir.join(path);
                execute(
                    response,
                    // a path can go over multiple json node!
                    &current_dir,
                    node,
                    request.clone(),
                    service_registry.clone(),
                )
                .await
            }
        }
    })
}

async fn fetch_node<'a>(
    response: &'a GraphQLResponse,
    current_dir: &'a Path,
    FetchNode {
        variable_usages,
        requires,
        operation,
        service_name,
    }: &'a FetchNode,
    request: Arc<GraphQLRequest>,
    service_registry: Arc<dyn ServiceRegistry>,
) -> Result<GraphQLResponse, FetchError> {
    if let Some(requires) = requires {
        // We already checked that the service exists during planning
        let fetcher = service_registry.get(service_name).unwrap();

        let mut variables = Object::with_capacity(1 + variable_usages.len());
        variables.extend(variable_usages.iter().filter_map(|key| {
            request
                .variables
                .get(key)
                .map(|value| (key.to_owned(), value.to_owned()))
        }));

        {
            log::trace!(
                "Creating representations at path '{}' for selections={:?} using data={}",
                current_dir,
                requires,
                serde_json::to_string(&response.data).unwrap(),
            );
            let representations = response.select(current_dir, requires)?;
            debug_assert!(!representations.as_array().unwrap().is_empty());
            variables.insert("representations".into(), representations);
        }

        let (res, _tail) = fetcher
            .stream(
                GraphQLRequest::builder()
                    .query(operation)
                    .variables(variables)
                    .build(),
            )
            .into_future()
            .await;

        match res {
            Some(response) if !response.is_primary() => {
                Err(FetchError::SubrequestUnexpectedPatchResponse {
                    service: service_name.to_owned(),
                })
            }
            Some(GraphQLResponse { data, .. }) => {
                if let Some(entities) = data.get("_entities") {
                    log::trace!(
                        "Received entities: {}",
                        serde_json::to_string(entities).unwrap(),
                    );
                    if let Some(array) = entities.as_array() {
                        let mut result = response.clone();

                        for (i, entity) in array.iter().enumerate() {
                            result.insert_data(
                                &current_dir.join(Path::from(i.to_string())),
                                entity.to_owned(),
                            )?;
                        }

                        Ok(result)
                    } else {
                        Err(FetchError::ExecutionInvalidContent {
                            reason: "Received invalid type for key `_entities`!".to_string(),
                        })
                    }
                } else {
                    Err(FetchError::ExecutionInvalidContent {
                        reason: "Missing key `_entities`!".to_string(),
                    })
                }
            }
            None => Err(FetchError::SubrequestNoResponse {
                service: service_name.to_string(),
            }),
        }
    } else {
        let variables = Arc::new(
            variable_usages
                .iter()
                .filter_map(|key| {
                    request
                        .variables
                        .get(key)
                        .map(|value| (key.to_owned(), value.to_owned()))
                })
                .collect::<Object>(),
        );

        // We already validated that the service exists during planning
        let fetcher = service_registry.get(service_name).unwrap();

        let (res, _tail) = fetcher
            .stream(
                GraphQLRequest::builder()
                    .query(operation.clone())
                    .variables(variables.clone())
                    .build(),
            )
            .into_future()
            .await;

        match res {
            Some(response) if !response.is_primary() => {
                Err(FetchError::SubrequestUnexpectedPatchResponse {
                    service: service_name.to_owned(),
                })
            }
            Some(GraphQLResponse { data, .. }) => {
                let mut result = response.clone();
                result.insert_data(current_dir, data)?;
                Ok(result)
            }
            None => Err(FetchError::SubrequestNoResponse {
                service: service_name.to_string(),
            }),
        }
    }
}
