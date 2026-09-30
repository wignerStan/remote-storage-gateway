#include "infra_plugin_v1.h"
#include <stdlib.h>
#include <string.h>

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
    { sizeof(InfraPluginV1), INFRA_PLUGIN_ABI_VERSION, INFRA_PLUGIN_FUNCTION,
      INFRA_LANGUAGE_RUST, INFRA_UNLOAD_EXPLICIT,
      { (const uint8_t *)"worker.metadata.test", 20 }, { (const uint8_t *)"native", 6 } },
    init, open_callback, call, next, close_callback, destroy, free_reply
};
const InfraPluginV1 *infra_plugin_v1_entry(void) { return &table; }
