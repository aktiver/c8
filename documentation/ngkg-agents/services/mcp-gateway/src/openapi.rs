//! Served OpenAPI contract and Swagger UI for the complete gateway REST surface.

use axum::{
    Router,
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};

const OPENAPI: &str = include_str!("../../../contracts/mcp-agent-openapi.yaml");
const SWAGGER: &str = r#"<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>NGKG MCP/Agent API</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.31.0/swagger-ui.css"><style>body{margin:0}</style></head><body><div id="swagger-ui"></div><script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.31.0/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/openapi.yaml',dom_id:'#swagger-ui',deepLinking:true,displayRequestDuration:true,persistAuthorization:false,tryItOutEnabled:true})</script></body></html>"#;

pub(crate) fn router() -> Router {
    Router::new()
        .route("/openapi.yaml", get(spec))
        .route("/swagger-ui", get(swagger))
}
async fn spec() -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/yaml"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        OPENAPI,
    )
        .into_response()
}
async fn swagger() -> Response {
    (StatusCode::OK,[(header::CONTENT_SECURITY_POLICY,"default-src 'none'; style-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; script-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src https://cdn.jsdelivr.net"),(header::CACHE_CONTROL,"no-store")],Html(SWAGGER)).into_response()
}
