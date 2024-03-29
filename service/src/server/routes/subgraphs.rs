use axum::{
    extract::Extension,
    http::{self, HeaderName, Request, StatusCode},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use tracing::trace;

use crate::{
    query_processor::{FreeQuery, SubgraphDeploymentID},
    server::{
        routes::{bad_request_response, response_body_to_query_string},
        ServerOptions,
    },
};

pub async fn subgraph_queries(
    Extension(server): Extension<ServerOptions>,
    id: axum::extract::Path<String>,
    req: Request<axum::body::Body>,
) -> impl IntoResponse {
    // Extract scalar receipt from header and free query auth token for paid or free query
    let receipt = if let Some(recipt) = req.headers().get("scalar-receipt") {
        match recipt.to_str() {
            Ok(r) => Some(r),
            Err(_) => {
                return bad_request_response("Bad scalar receipt for subgraph query");
            }
        }
    } else {
        None
    };
    trace!(
        "receipt attached by the query, can pass it to TAP: {:?}",
        receipt
    );

    // Extract free query auth token
    let auth_token = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|t| t.to_str().ok());
    // determine if the query is paid or authenticated to be free
    let free = auth_token.is_some()
        && server.free_query_auth_token.is_some()
        && auth_token.unwrap() == server.free_query_auth_token.as_deref().unwrap();

    let query_string = match response_body_to_query_string(req.into_body()).await {
        Ok(q) => q,
        Err(e) => return bad_request_response(&e.to_string()),
    };

    // Initialize id into a subgraph deployment ID
    let subgraph_deployment_id = SubgraphDeploymentID::new(Arc::new(id).to_string());

    if free {
        let free_query = FreeQuery {
            subgraph_deployment_id,
            query: query_string,
        };
        let res = server
            .query_processor
            .execute_free_query(free_query)
            .await
            .expect("Failed to execute free query");

        match res.status {
            200 => {
                let response_body = res.result.graphql_response;
                let attestable = res.result.attestable;
                (
                    StatusCode::OK,
                    axum::response::AppendHeaders([(
                        HeaderName::from_static("graph-attestable"),
                        if attestable { "true" } else { "false" },
                    )]),
                    Json(response_body),
                )
                    .into_response()
            }
            _ => bad_request_response("Bad response from Graph node"),
        }
    } else {
        let error_body = "Query request header missing scalar-receipts or incorrect auth token";
        bad_request_response(error_body)
    }
}
