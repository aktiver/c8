//! Gateway adapter for the shared, explicitly selected authentication mode.

use axum::{
    Json,
    extract::Request,
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use ngkg_auth::{Authenticator, Identity};
use serde::Serialize;

pub(crate) type GatewayIdentity = Identity;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthErrorResponse {
    code: &'static str,
    message: &'static str,
}

pub(crate) async fn require_authentication(
    authenticator: Authenticator,
    mut request: Request,
    next: Next,
) -> Response {
    let authenticated = match authenticator.authenticate(request.headers()).await {
        Ok(value) => value,
        Err(error) => {
            let status = if matches!(error, ngkg_auth::AuthError::Unavailable) {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::UNAUTHORIZED
            };
            return (
                status,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                Json(AuthErrorResponse {
                    code: if status == StatusCode::UNAUTHORIZED {
                        "unauthenticated"
                    } else {
                        "authentication_unavailable"
                    },
                    message: if status == StatusCode::UNAUTHORIZED {
                        "valid bearer authentication is required"
                    } else {
                        "authentication dependency is unavailable"
                    },
                }),
            )
                .into_response();
        }
    };
    // In exchange mode the external OAuth token is dropped here and never
    // reaches NGKG, an audit record, a tool result, or a log field.
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, authenticated.upstream_authorization);
    request.extensions_mut().insert(authenticated.identity);
    next.run(request).await
}
