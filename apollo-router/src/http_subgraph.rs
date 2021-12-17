use apollo_router_core::prelude::*;
use async_trait::async_trait;
use bytes::BytesMut;
use derivative::Derivative;
use futures::{lock::Mutex, prelude::*};
use std::{collections::HashMap, pin::Pin};
use tokio::sync::broadcast::{self, Sender};
use tracing::Instrument;

type BytesStream = Pin<
    Box<dyn futures::Stream<Item = Result<bytes::Bytes, graphql::FetchError>> + std::marker::Send>,
>;

/// A fetcher for subgraph data that uses http.
/// Streaming via chunking is supported.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct HttpSubgraphFetcher {
    service: String,
    url: String,
    #[derivative(Debug = "ignore")]
    http_client: reqwest_middleware::ClientWithMiddleware,
    #[derivative(Debug = "ignore")]
    wait_map: Mutex<HashMap<String, Sender<graphql::Response>>>,
}

impl HttpSubgraphFetcher {
    /// Construct a new http subgraph fetcher that will fetch from the supplied URL.
    pub fn new(service: String, url: String) -> Self {
        HttpSubgraphFetcher {
            http_client: reqwest_middleware::ClientBuilder::new(
                reqwest::Client::builder()
                    .tcp_keepalive(Some(std::time::Duration::from_secs(5)))
                    .build()
                    .unwrap(),
            )
            .with(reqwest_tracing::TracingMiddleware)
            .with(LoggingMiddleware::new(&service))
            .build(),
            service,
            url,
            wait_map: Mutex::new(HashMap::new()),
        }
    }

    fn request_stream(&self, request: graphql::Request) -> BytesStream {
        // Perform the actual request and start streaming.
        // Reqwest doesn't care if there is only one response, in this case it'll be a stream of
        // one element.
        let service = self.service.clone();
        self.http_client
            .post(self.url.clone())
            .json(&request)
            .send()
            .instrument(tracing::trace_span!("http-subgraph-request"))
            // We have a future for the response, convert it to a future of the stream.
            .map_ok(|r| r.bytes_stream().boxed())
            // Convert the entire future to a stream, at this point we have a stream of a result of
            // a single stream
            .into_stream()
            // Flatten the stream
            .flat_map(|result| match result {
                Ok(s) => s.map_err(Into::into).boxed(),
                Err(err) => stream::iter(vec![Err(err)]).boxed(),
            })
            .map_err(move |err: reqwest_middleware::Error| {
                tracing::error!(fetch_error = format!("{:?}", err).as_str());

                graphql::FetchError::SubrequestHttpError {
                    service: service.to_owned(),
                    reason: err.to_string(),
                }
            })
            .boxed()
    }

    fn map_to_graphql(
        service_name: String,
        mut bytes_stream: BytesStream,
    ) -> graphql::ResponseStream {
        Box::pin(
            async move {
                let mut current_payload_bytes = BytesMut::new();
                let is_primary = true;

                while let Some(next_chunk) = bytes_stream.next().await {
                    match next_chunk {
                        Ok(bytes) => {
                            current_payload_bytes.extend(&bytes);
                        }
                        Err(fetch_error) => {
                            return fetch_error.to_response(is_primary);
                        }
                    }
                }

                serde_json::from_slice::<graphql::Response>(&current_payload_bytes).unwrap_or_else(
                    |error| {
                        graphql::FetchError::SubrequestMalformedResponse {
                            service: service_name.clone(),
                            reason: error.to_string(),
                        }
                        .to_response(is_primary)
                    },
                )
            }
            .into_stream(),
        )
    }

    async fn dedup(&self, request: graphql::Request) -> graphql::ResponseStream {
        let hashed_request = serde_json::to_string(&request).unwrap();

        let mut locked_wait_map = self.wait_map.lock().await;
        let res = match locked_wait_map.get_mut(&hashed_request) {
            Some(waiter) => {
                // Register interest in key
                let mut receiver = waiter.subscribe();
                drop(locked_wait_map);
                let recv_value = receiver.recv().await.expect("FIXME");
                recv_value
            }
            None => {
                let (tx, _rx) = broadcast::channel(1);
                locked_wait_map.insert(hashed_request.clone(), tx.clone());
                drop(locked_wait_map);

                let service_name = self.service.to_string();
                let bytes_stream = self.request_stream(request);
                let res = Self::map_to_graphql(service_name, bytes_stream)
                    .next()
                    .await
                    .unwrap();

                let mut locked_wait_map = self.wait_map.lock().await;
                locked_wait_map.remove(&hashed_request);
                // Let our waiters know
                let broadcast_value = res.clone();
                // Our use case is very specific, so we are sure that
                // we won't get any errors here.
                tokio::task::spawn_blocking(move || {
                    tx.send(broadcast_value)
                        .expect("there is always at least one receiver alive, the _rx guard; qed")
                }).await
                .expect("can only fail if the task is aborted or if the internal code panics, neither is possible here; qed");
                res
            }
        };

        Box::pin(async { res }.into_stream())
    }
}

#[async_trait]
impl graphql::Fetcher for HttpSubgraphFetcher {
    /// Using reqwest fetch a stream of graphql results.
    async fn stream(&self, request: graphql::Request) -> graphql::ResponseStream {
        self.dedup(request).await
    }
}

struct LoggingMiddleware {
    service: String,
}

impl LoggingMiddleware {
    fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
        }
    }
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for LoggingMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut task_local_extensions::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        tracing::trace!("Request to service {}: {:?}", self.service, req);
        let res = next.run(req, extensions).await;
        tracing::trace!("Response from service {}: {:?}", self.service, res);
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::POST;
    use httpmock::{MockServer, Regex};
    use serde_json::json;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_non_chunked() -> Result<(), Box<dyn std::error::Error>> {
        let response = graphql::Response::builder()
            .data(json!({
              "allProducts": [
                {
                  "variation": {
                    "id": "OSS"
                  },
                  "id": "apollo-federation"
                },
                {
                  "variation": {
                    "id": "platform"
                  },
                  "id": "apollo-studio"
                }
              ]
            }))
            .build();

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/graphql")
                .body_matches(Regex::new(".*").unwrap());
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body_obj(&response);
        });
        let fetcher = HttpSubgraphFetcher::new("products".into(), server.url("/graphql"));
        let collect = fetcher
            .stream(
                graphql::Request::builder()
                    .query(r#"{allProducts{variation {id}id}}"#)
                    .build(),
            )
            .await
            .collect::<Vec<_>>()
            .await;

        assert_eq!(collect[0], response);
        mock.assert();
        Ok(())
    }
}
