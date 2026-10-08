#define _POSIX_C_SOURCE 200809L
#include "infra_plugin_v1.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#ifndef TEST_MODE
#define TEST_MODE 0
#endif
#ifndef TEST_CODEC_PREFIX
#define TEST_CODEC_PREFIX "openai.responses.v1"
#endif
static unsigned requests, completed;
static uint32_t copy(const void *data, size_t len, InfraOwnedBytes *out) {
    out->ptr = len ? malloc(len) : NULL; out->len = len;
    if (len && !out->ptr) return INFRA_FAILED;
    if (len) memcpy(out->ptr, data, len);
    return INFRA_OK;
}
static uint32_t action(const char *metadata, const char *payload, InfraOwnedBytes *out) {
    size_t n = strlen(metadata), p = strlen(payload);
    out->ptr = malloc(5 + n + p); out->len = 5 + n + p;
    if (!out->ptr) return INFRA_FAILED;
    out->ptr[0] = 2;
    for (unsigned i = 0; i != 4; ++i) out->ptr[1 + i] = (uint8_t)(n >> (8 * i));
    memcpy(out->ptr + 5, metadata, n); memcpy(out->ptr + 5 + n, payload, p);
    return INFRA_OK;
}
static int begins(InfraBytes bytes, const char *prefix) {
    size_t n = strlen(prefix); return bytes.len >= n && !memcmp(bytes.ptr, prefix, n);
}
static uint32_t init(const InfraHostV1 *host, uint64_t *state) { (void)host; *state = 1; return INFRA_OK; }
static uint32_t open_object(uint64_t state, uint64_t binding, InfraBytes input, uint64_t *handle) {
    (void)state; (void)input; if (binding) return INFRA_FAILED;
    requests = completed = 0; *handle = 1; return INFRA_OK;
}
static uint32_t call(uint64_t state, uint64_t handle, InfraBytes method, InfraBytes input, InfraCallMeta meta, InfraOwnedBytes *out) {
    (void)state; (void)handle; (void)meta;
#if TEST_MODE >= 3
    if (begins(method, TEST_CODEC_PREFIX "/transport.closed/")) {
        if (!begins(input, "{\"side\":\"downstream\"")) return INFRA_FAILED;
        FILE *marker = fopen(TEST_MARKER_PATH, "a");
        if (!marker) return INFRA_FAILED;
        fputs("closed\n", marker); fclose(marker);
#if TEST_MODE == 3
        const uint8_t deny = 1; return copy(&deny, 1, out);
#elif TEST_MODE == 4
        return INFRA_FAILED;
#elif TEST_MODE == 5
        return action("{\"version\":1,\"decision\":\"reply\"}", "ignored teardown output", out);
#else
        const uint8_t allow = 0; return copy(&allow, 1, out);
#endif
    }
#endif
    if (begins(method, TEST_CODEC_PREFIX "/response.create/")) {
        if (!input.len || input.ptr[0] != '{') return INFRA_FAILED;
        ++requests;
#if TEST_MODE == 2
        struct timespec delay = {0, 150000000}; nanosleep(&delay, NULL);
#endif
#if TEST_MODE == 1
        return action("{\"version\":1,\"decision\":\"forward\",\"set_headers\":[[\"host\",\"ungranted.invalid\"]]}", "", out);
#else
        if (requests == 1) return action(
            "{\"version\":1,\"decision\":\"forward\",\"upstream\":\"connect\",\"path\":\"/selected?generation=1\",\"set_headers\":[[\"cookie\",\"session=one\"],[\"x-request-id\",\"request-one\"]]}",
            "{\"type\":\"response.create\",\"model\":\"approved-one\",\"input\":[]}", out);
        if (requests == 2) return action(
            "{\"version\":1,\"decision\":\"forward\",\"upstream\":\"connect\",\"path\":\"/selected?generation=2\",\"set_headers\":[[\"cookie\",\"session=two\"],[\"x-request-id\",\"request-two\"]]}",
            "{\"type\":\"response.create\",\"model\":\"approved-two\",\"input\":[]}", out);
        return INFRA_FAILED;
#endif
    }
    if (begins(method, TEST_CODEC_PREFIX "/response.completed/")) {
        if (++completed == 1) return action(
            "{\"version\":1,\"decision\":\"reply\",\"upstream\":\"disconnect\"}",
            "{\"type\":\"error\",\"status\":409,\"error\":{\"type\":\"invalid_request_error\",\"code\":\"previous_response_not_found\",\"message\":\"Replay the full request\"}}", out);
        const uint8_t allow = 0; return copy(&allow, 1, out);
    }
    /* A delta callback is a host regression, even with action/response grants. */
    if (begins(method, TEST_CODEC_PREFIX "/response.event/")) {
        FILE *marker = fopen(TEST_DELTA_MARKER_PATH, "a");
        if (marker) { fputs("unexpected delta\n", marker); fclose(marker); }
    }
    return INFRA_FAILED;
}
static uint32_t next(uint64_t state, uint64_t handle, uint32_t credit, InfraCallMeta meta, InfraOwnedBytes *out, uint32_t *eof) {
    (void)state; (void)handle; (void)credit; (void)meta; *out = (InfraOwnedBytes){0}; *eof = 1; return INFRA_OK;
}
static uint32_t close_object(uint64_t state, uint64_t handle) { (void)state; (void)handle; return INFRA_OK; }
static uint32_t destroy(uint64_t state) { (void)state; return INFRA_OK; }
static void free_reply(uint64_t state, InfraOwnedBytes out) { (void)state; free(out.ptr); }
static const InfraPluginV1 table = {
    {sizeof(InfraPluginV1), INFRA_PLUGIN_ABI_VERSION, INFRA_PLUGIN_OBJECT, INFRA_LANGUAGE_RUST, INFRA_UNLOAD_EXPLICIT,
     {(const uint8_t *)"worker.sessions.test", 20}, {(const uint8_t *)"native", 6}},
    init, open_object, call, next, close_object, destroy, free_reply
};
const InfraPluginV1 *infra_plugin_v1_entry(void) { return &table; }
