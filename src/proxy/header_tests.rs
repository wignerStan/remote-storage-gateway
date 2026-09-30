use super::*;

#[test]
fn request_framing_is_derived_from_native_input_and_sealed_body_plan() {
    let mut original = RequestHeader::build("POST", b"/upload?q=1", None).unwrap();
    original.insert_header("content-length", "6").unwrap();
    let upstream: Url = "http://origin.test".parse().unwrap();
    for variable in [false, true] {
        let mut outgoing = original.clone();
        outgoing.insert_header("content-length", "1").unwrap();
        outgoing
            .insert_header("proxy-authorization", "secret")
            .unwrap();
        request_head(&mut outgoing, &original, &upstream, true, variable, true).unwrap();
        assert_eq!(original.headers["content-length"], "6");
        assert!(!outgoing.headers.contains_key("proxy-authorization"));
        assert_eq!(outgoing.headers["host"], "origin.test");
        if variable {
            assert!(!outgoing.headers.contains_key("content-length"));
            assert_eq!(outgoing.headers["transfer-encoding"], "chunked");
        } else {
            assert_eq!(outgoing.headers["content-length"], "6");
        }
        wire_request(&mut outgoing, Version::HTTP_2, true, true).unwrap();
        assert_eq!(outgoing.version, Version::HTTP_2);
        assert!(!outgoing.headers.contains_key("transfer-encoding"));
        assert!(!outgoing.headers.contains_key("connection"));
    }
}

#[test]
fn parsed_ingress_rejects_ambiguous_framing_and_connection_nominated_lengths() {
    let mut request = RequestHeader::build("POST", b"/", None).unwrap();
    request.insert_header("content-length", "6").unwrap();
    request
        .insert_header("transfer-encoding", "chunked")
        .unwrap();
    assert!(validate_ingress(&request).is_err());
    request.remove_header("transfer-encoding");
    assert!(validate_ingress(&request).is_ok());
    request
        .insert_header("connection", "content-length")
        .unwrap();
    assert!(
        request_head(
            &mut request.clone(),
            &request,
            &"http://origin.test".parse().unwrap(),
            true,
            false,
            true
        )
        .is_err()
    );
}

#[test]
fn response_framing_cannot_be_forged_by_callbacks() {
    let mut response = ResponseHeader::build(200, None).unwrap();
    response.insert_header("content-length", "6").unwrap();
    let original = ResponseFraming::capture(&response);
    response.insert_header("content-length", "1").unwrap();
    response_head(
        &mut response,
        &original,
        &http::Method::GET,
        Version::HTTP_11,
        false,
    )
    .unwrap();
    assert_eq!(response.headers["content-length"], "6");
    response_head(
        &mut response,
        &original,
        &http::Method::GET,
        Version::HTTP_11,
        true,
    )
    .unwrap();
    assert!(!response.headers.contains_key("content-length"));
    assert_eq!(response.headers["transfer-encoding"], "chunked");
    response.set_status(101).unwrap();
    assert!(original.validate(&response, &http::Method::GET).is_err());
    response.set_status(204).unwrap();
    assert!(original.validate(&response, &http::Method::GET).is_err());
}

#[test]
fn header_only_responses_keep_representation_metadata_without_fabricated_body() {
    for (status, method) in [
        (200, http::Method::HEAD),
        (304, http::Method::GET),
        (204, http::Method::GET),
    ] {
        let mut response = ResponseHeader::build(status, None).unwrap();
        response.insert_header("content-length", "42").unwrap();
        let original = ResponseFraming::capture(&response);
        response_head(&mut response, &original, &method, Version::HTTP_11, false).unwrap();
        assert!(!response.headers.contains_key("transfer-encoding"));
        assert_eq!(
            response.headers.contains_key("content-length"),
            status != 204
        );
    }
    let mut response = ResponseHeader::build(200, None).unwrap();
    response.insert_header("content-length", "999").unwrap();
    response
        .insert_header("transfer-encoding", "chunked")
        .unwrap();
    let original = ResponseFraming::capture(&response);
    response_head(
        &mut response,
        &original,
        &http::Method::GET,
        Version::HTTP_11,
        false,
    )
    .unwrap();
    assert!(!response.headers.contains_key("content-length"));
    assert_eq!(response.headers["transfer-encoding"], "chunked");
}
