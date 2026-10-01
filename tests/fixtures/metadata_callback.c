#include "infra_plugin_v1.h"
#include <stdlib.h>
#include <string.h>

/* ABI-shaped C fixture; Go identity exercises loader policy, not Go runtime code. */
#ifndef TEST_PLUGIN_KIND
#define TEST_PLUGIN_KIND INFRA_PLUGIN_FUNCTION
#endif
#ifndef TEST_PLUGIN_LANGUAGE
#define TEST_PLUGIN_LANGUAGE INFRA_LANGUAGE_RUST
#endif
#ifndef TEST_UNLOAD_POLICY
#define TEST_UNLOAD_POLICY INFRA_UNLOAD_EXPLICIT
#endif

/* Test-only retained snapshots let Rust inspect the actual native callback frame. */
static uint8_t *reply_bytes, *last_frame;
static size_t reply_len, frame_len;
static uint32_t copy_reply(const uint8_t *bytes, size_t len, InfraOwnedBytes *out) {
    out->ptr = len ? malloc(len) : NULL;
    out->len = len;
    if (len && !out->ptr) return INFRA_FAILED;
    if (len) memcpy(out->ptr, bytes, len);
    return INFRA_OK;
}
static uint32_t replace(uint8_t **owned, size_t *length, InfraBytes value) {
    uint8_t *next = value.len ? malloc(value.len) : NULL;
    if (value.len && !next) return INFRA_FAILED;
    if (value.len) memcpy(next, value.ptr, value.len);
    free(*owned);
    *owned = next;
    *length = value.len;
    return INFRA_OK;
}
static uint32_t init(const InfraHostV1 *host, uint64_t *state) {
    (void)host; *state = 1; return INFRA_OK;
}
static uint32_t open_callback(uint64_t state, uint64_t binding, InfraBytes input, uint64_t *handle) {
    (void)state;
    if (binding || input.len < 10 || memcmp(input.ptr, "IPB1\0\0", 6)) return INFRA_FAILED;
    size_t len = ((size_t)input.ptr[6] << 24) | ((size_t)input.ptr[7] << 16)
        | ((size_t)input.ptr[8] << 8) | input.ptr[9];
    if (len != input.len - 10) return INFRA_FAILED;
    uint32_t result = replace(&reply_bytes, &reply_len, (InfraBytes){ input.ptr + 10, len });
    if (result == INFRA_OK) *handle = 1;
    return result;
}
static uint32_t call(uint64_t state, uint64_t handle, InfraBytes method, InfraBytes input,
        InfraCallMeta meta, InfraOwnedBytes *out) {
    (void)state; (void)handle; (void)meta;
    if (method.len == 7 && !memcmp(method.ptr, "inspect", 7))
        return copy_reply(last_frame, frame_len, out);
    if (method.len == 9 && !memcmp(method.ptr, "set_reply", 9)) {
        uint32_t result = replace(&reply_bytes, &reply_len, input);
        *out = (InfraOwnedBytes){0};
        return result;
    }
#ifdef TEST_RESPONSE_REPLY
#ifndef TEST_RESPONSE_REQUEST_METHOD
#define TEST_RESPONSE_REQUEST_METHOD "openai.chat_completions.v1/chat_completion/"
#endif
    {
        const char *request_prefix = TEST_RESPONSE_REQUEST_METHOD;
        const char *response_prefix = TEST_RESPONSE_METHOD;
        const uint8_t allow[] = {0};
        if (method.len == strlen(request_prefix) + 33 && !memcmp(method.ptr, request_prefix, strlen(request_prefix)))
            return copy_reply(allow, sizeof(allow), out);
        if (method.len != strlen(response_prefix) + 33 || memcmp(method.ptr, response_prefix, strlen(response_prefix))) return INFRA_FAILED;
        const char *expected = TEST_RESPONSE_BODY;
        if (input.len != strlen(expected) || memcmp(input.ptr, expected, input.len)) return INFRA_FAILED;
        const uint8_t action[] = {TEST_RESPONSE_REPLY};
        return copy_reply(action, sizeof(action), out);
    }
#endif
#ifdef TEST_REALTIME_REPLY
    {
        const char *prefix = TEST_REALTIME_METHOD;
        const char *body = TEST_REALTIME_BODY;
        size_t n = strlen(prefix);
        if (method.len != n + 33 || memcmp(method.ptr, prefix, n)
                || input.len != strlen(body) || memcmp(input.ptr, body, input.len)) return INFRA_FAILED;
        const uint8_t action[] = {TEST_REALTIME_REPLY};
        return copy_reply(action, sizeof(action), out);
    }
#endif
#ifdef TEST_RESPONSES_REPLY
    {
        static size_t sequence;
        const char *kind = sequence == 1 ? "responses_lite.delta/" : "response.create/";
        const char *prefix = TEST_API_PREFIX;
        size_t n = strlen(prefix), k = strlen(kind);
        if (sequence > 2 || method.len != n + k + 33 || memcmp(method.ptr, prefix, n)
                || memcmp(method.ptr + n, kind, k) || method.ptr[method.len - 1] != '0' + sequence
                || input.len < 2 || input.ptr[0] != '{') return INFRA_FAILED;
        sequence++;
        const uint8_t action[] = {0};
        return copy_reply(action, sizeof(action), out);
    }
#endif
#ifdef TEST_RESPONSES_ERROR
    {
        /* First response.create is the CLIProxyAPI previous-response document.
           The next one on the same socket is allowed through. */
        static size_t sequence;
        const char *kind = "response.create/";
        const char *prefix = TEST_API_PREFIX;
        size_t n = strlen(prefix), k = strlen(kind);
        if (sequence > 1 || method.len != n + k + 33 || memcmp(method.ptr, prefix, n)
                || memcmp(method.ptr + n, kind, k) || method.ptr[method.len - 1] != '0' + sequence
                || input.len < 2 || input.ptr[0] != '{') return INFRA_FAILED;
        sequence++;
        if (sequence == 1) {
            static const char document[] =
                "{\"type\":\"error\",\"status\":409,\"error\":{\"message\":\"Previous response was not found. Retrying the full request.\",\"type\":\"invalid_request_error\",\"code\":\"previous_response_not_found\",\"param\":\"previous_response_id\"}}";
            return copy_reply((const uint8_t *)document, sizeof(document) - 1, out);
        }
        const uint8_t action[] = {0};
        return copy_reply(action, sizeof(action), out);
    }
#endif
#ifdef TEST_RESPONSES_TERMINAL
    {
        /* Deltas are not offered when message_responses is off. A completion
           whose payload contains "cached_tokens":0 is the replay document.
           Any other completion is forwarded. The socket stays open. */
        const char *prefix = TEST_API_PREFIX;
        size_t n = strlen(prefix);
        if (method.len < n || memcmp(method.ptr, prefix, n) || input.len < 2 || input.ptr[0] != '{')
            return INFRA_FAILED;
        const char *rest = method.ptr + n;
        size_t rest_len = method.len - n;
        if (rest_len >= 16 && !memcmp(rest, "response.create/", 16)) {
            const uint8_t action[] = {0};
            return copy_reply(action, sizeof(action), out);
        }
        if ((rest_len >= 15 && !memcmp(rest, "response.event/", 15))
                || (rest_len >= 16 && !memcmp(rest, "response.binary/", 16)))
            return INFRA_FAILED;
        if (rest_len >= 19 && !memcmp(rest, "response.completed/", 19)) {
            const char needle[] = "\"cached_tokens\":0";
            size_t m = sizeof(needle) - 1;
            int zero = 0;
            if (input.len >= m) {
                for (size_t i = 0; i + m <= input.len; i++) {
                    if (!memcmp(input.ptr + i, needle, m)) {
                        zero = 1;
                        break;
                    }
                }
            }
            if (zero) {
                static const char document[] =
                    "{\"type\":\"error\",\"status\":409,\"error\":{\"message\":\"Previous response was not found. Retrying the full request.\",\"type\":\"invalid_request_error\",\"code\":\"previous_response_not_found\",\"param\":\"previous_response_id\"}}";
                return copy_reply((const uint8_t *)document, sizeof(document) - 1, out);
            }
            const uint8_t action[] = {0};
            return copy_reply(action, sizeof(action), out);
        }
        if (rest_len >= 14 && !memcmp(rest, "response.done/", 14)) {
            const uint8_t action[] = {0};
            return copy_reply(action, sizeof(action), out);
        }
        return INFRA_FAILED;
    }
#endif
#ifdef TEST_RESPONSES_BAD
    {
        const char *kind = "response.create/";
        const char *prefix = TEST_API_PREFIX;
        size_t n = strlen(prefix), k = strlen(kind);
        if (method.len != n + k + 33 || memcmp(method.ptr, prefix, n)
                || memcmp(method.ptr + n, kind, k) || input.len < 2 || input.ptr[0] != '{')
            return INFRA_FAILED;
        static const char document[] = "{\"type\":\"response.completed\"}";
        return copy_reply((const uint8_t *)document, sizeof(document) - 1, out);
    }
#endif
#ifdef TEST_API_REPLY
    {
        const char *prefix = TEST_API_PREFIX;
#if TEST_GEMINI_BODY
        size_t n = strlen(prefix);
        const char *kind = "generate_content/";
        const char *stream_kind = "stream_generate_content/";
        if (!((method.len == n + strlen(kind) + 33 && !memcmp(method.ptr + n, kind, strlen(kind)))
                || (method.len == n + strlen(stream_kind) + 33 && !memcmp(method.ptr + n, stream_kind, strlen(stream_kind))))) return INFRA_FAILED;
        const char *body = "{\"contents\":[{\"parts\":[{\"text\":\"hello\"}]}],\"unknown\":\"preserved\"}";
#else
        const char *body = "{\"messages\":[],\"unknown\":\"preserved\"}";
#endif
        if (method.len < strlen(prefix) || memcmp(method.ptr, prefix, strlen(prefix))
                || input.len != strlen(body) || memcmp(input.ptr, body, input.len)) return INFRA_FAILED;
        const uint8_t action[] = {TEST_API_REPLY};
        return copy_reply(action, sizeof(action), out);
    }
#endif
#ifdef TEST_WEBSOCKET_REPLY
    if (method.len >= 10 && !memcmp(method.ptr, "websocket.", 10)) {
        const char *prefix = "websocket.v1/text/";
        size_t n = strlen(prefix);
        if (method.len != n + 33 || memcmp(method.ptr, prefix, n)
                || method.ptr[n + 16] != '/' || !frame_len
                || input.len != 2 || memcmp(input.ptr, "hi", 2)) return INFRA_FAILED;
        for (size_t i = n; i < method.len; i++) {
            if (i == n + 16) continue;
            uint8_t c = method.ptr[i];
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return INFRA_FAILED;
        }
        const uint8_t action[] = {TEST_WEBSOCKET_REPLY};
        return copy_reply(action, sizeof(action), out);
    }
#endif
    uint32_t result = replace(&last_frame, &frame_len, input);
    if (result != INFRA_OK) return result;
    if (reply_len == 4 && !memcmp(reply_bytes, "FAIL", 4)) return INFRA_FAILED;
    return copy_reply(reply_bytes, reply_len, out);
}
static uint32_t next(uint64_t state, uint64_t handle, uint32_t credit, InfraCallMeta meta,
        InfraOwnedBytes *out, uint32_t *eof) {
    (void)state; (void)handle; (void)credit; (void)meta;
    *out = (InfraOwnedBytes){0}; *eof = 1; return INFRA_OK;
}
static uint32_t close_callback(uint64_t state, uint64_t handle) {
    (void)state; (void)handle;
    free(reply_bytes); reply_bytes = NULL; reply_len = 0;
    free(last_frame); last_frame = NULL; frame_len = 0;
    return INFRA_OK;
}
static uint32_t destroy(uint64_t state) { (void)state; return INFRA_OK; }
static void free_reply(uint64_t state, InfraOwnedBytes reply) { (void)state; free(reply.ptr); }
static const InfraPluginV1 table = {
    { sizeof(InfraPluginV1), INFRA_PLUGIN_ABI_VERSION, TEST_PLUGIN_KIND,
      TEST_PLUGIN_LANGUAGE, TEST_UNLOAD_POLICY,
      { (const uint8_t *)"worker.metadata.test", 20 }, { (const uint8_t *)"native", 6 } },
    init, open_callback, call, next, close_callback, destroy, free_reply
};
const InfraPluginV1 *infra_plugin_v1_entry(void) { return &table; }
