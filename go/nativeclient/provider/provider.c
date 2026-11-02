#define _GNU_SOURCE
#include "provider.h"
#include "_cgo_export.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

typedef struct WorkerClientOperation WorkerClientOperation;
typedef struct {
    pthread_mutex_t lock;
    WorkerClientOperation *operations;
    uint64_t service, next_id, generation;
    unsigned count;
    int closed;
} WorkerClientState;
static _Atomic uint64_t next_generation=1;
struct WorkerClientOperation {
    WorkerClientOperation *next;
    WorkerClientState *state;
    pthread_t worker;
    uint64_t id;
    int fd, taken;
    _Atomic int done, cancelled;
    InfraBytes method, input;
    InfraCallMeta meta;
    InfraOwnedBytes reply;
    uint32_t result;
};
int worker_client_operation_cancelled(void *token) {
    return atomic_load(&((WorkerClientOperation *)token)->cancelled);
}
static uint32_t provider_init(const InfraHostV1 *host, uint64_t *out) {
    (void)host;
    if (!out) return INFRA_FAILED;
    WorkerClientState *s=calloc(1,sizeof(*s));
    if (!s) return INFRA_FAILED;
    if (pthread_mutex_init(&s->lock,NULL)) {free(s);return INFRA_FAILED;}
    s->generation=atomic_fetch_add(&next_generation,1);
    if (!s->generation) {pthread_mutex_destroy(&s->lock);free(s);return INFRA_FAILED;}
    s->service=worker_client_go_new();
    if (!s->service) {pthread_mutex_destroy(&s->lock);free(s);return INFRA_FAILED;}
    s->next_id=1;*out=(uint64_t)(uintptr_t)s;return INFRA_OK;
}
static uint32_t provider_open(uint64_t state,uint64_t binding,InfraBytes input,uint64_t *out) {
    if (!state||binding||!out||input.len>(1u<<20)) return INFRA_FAILED;
    WorkerClientState *s=(void *)(uintptr_t)state;
    pthread_mutex_lock(&s->lock);int closed=s->closed;pthread_mutex_unlock(&s->lock);
    if (closed) return INFRA_CLOSED;
    *out=state;return INFRA_OK;
}
static uint32_t provider_call(uint64_t s,uint64_t h,InfraBytes m,InfraBytes in,InfraCallMeta meta,InfraOwnedBytes *out) {
    (void)s;(void)h;(void)m;(void)in;(void)meta;(void)out;return INFRA_FAILED;
}
static uint32_t provider_next(uint64_t s,uint64_t h,uint32_t credit,InfraCallMeta meta,InfraOwnedBytes *out,uint32_t *eof) {
    (void)s;(void)h;(void)credit;(void)meta;(void)out;(void)eof;return INFRA_FAILED;
}
static void *provider_worker(void *value) {
    WorkerClientOperation *op=value;
    op->result=worker_client_go_call(op->state->service,op->id,op->method,op->input,op->meta,op,&op->reply);
    atomic_store(&op->done,1);
    uint64_t one=1;(void)write(op->fd,&one,sizeof(one));return NULL;
}
static uint32_t reject(InfraAsyncStartV1 *out,int32_t code) {
    out->start.disposition=INFRA_PLUGIN_START_REJECTED;
    out->start.result_code=-code;return INFRA_PLUGIN_OK;
}
static uint32_t provider_start(InfraObjectCall call,InfraBytes method,InfraBytes input,InfraAsyncStartV1 *out) {
    if (!out) return INFRA_PLUGIN_INVALID_ARGUMENT;
    memset(out,0,sizeof(*out));out->readiness_fd=-1;
    if (!call.state||call.object!=call.state||!method.len||method.len>64||!method.ptr||input.len>(1u<<20)||(input.len&&!input.ptr)) return reject(out,INFRA_PLUGIN_INVALID_ARGUMENT);
    WorkerClientState *s=(void *)(uintptr_t)call.state;
    WorkerClientOperation *op=calloc(1,sizeof(*op));
    if (!op) return reject(out,INFRA_PLUGIN_EXHAUSTED);
    uint8_t *name=malloc(method.len);
    if (!name) {free(op);return reject(out,INFRA_PLUGIN_EXHAUSTED);}
    memcpy(name,method.ptr,method.len);op->method=(InfraBytes){name,method.len};op->input=input;op->meta=call.meta;op->state=s;
    op->fd=eventfd(0,EFD_CLOEXEC|EFD_NONBLOCK);
    if (op->fd<0) {free(name);free(op);return reject(out,INFRA_PLUGIN_SYSTEM_ERROR);}
    pthread_mutex_lock(&s->lock);
    if (s->closed||s->count>=128||!s->next_id) {
        pthread_mutex_unlock(&s->lock);close(op->fd);free(name);free(op);return reject(out,INFRA_PLUGIN_BUSY);
    }
    op->id=s->next_id++;op->next=s->operations;s->operations=op;s->count++;
    if (pthread_create(&op->worker,NULL,provider_worker,op)) {
        s->operations=op->next;s->count--;pthread_mutex_unlock(&s->lock);close(op->fd);free(name);free(op);return reject(out,INFRA_PLUGIN_SYSTEM_ERROR);
    }
    pthread_mutex_unlock(&s->lock);
    out->operation=(infra_plugin_handle_v1){op->id,s->generation};out->start.disposition=INFRA_PLUGIN_START_PENDING;out->readiness_fd=op->fd;return INFRA_PLUGIN_OK;
}
static WorkerClientOperation *find(WorkerClientState *s,infra_plugin_handle_v1 h) {
    if (h.generation!=s->generation) return NULL;
    for (WorkerClientOperation *op=s->operations;op;op=op->next) if (op->id==h.id) return op;
    return NULL;
}
static uint32_t provider_take(InfraObjectCall call,infra_plugin_handle_v1 h,InfraAsyncResultV1 *out) {
    if (!call.state||call.state!=call.object||!out) return INFRA_PLUGIN_INVALID_ARGUMENT;
    WorkerClientState *s=(void *)(uintptr_t)call.state;pthread_mutex_lock(&s->lock);WorkerClientOperation *op=find(s,h);
    if (!op||op->taken) {pthread_mutex_unlock(&s->lock);return INFRA_PLUGIN_STALE_HANDLE;}
    if (!atomic_load(&op->done)) {pthread_mutex_unlock(&s->lock);return INFRA_PLUGIN_BUSY;}
    pthread_join(op->worker,NULL);op->taken=1;out->result_code=op->result;out->reserved=0;out->reply=op->reply;op->reply=(InfraOwnedBytes){0};pthread_mutex_unlock(&s->lock);return INFRA_PLUGIN_OK;
}
static uint32_t provider_cancel(InfraObjectCall call,infra_plugin_handle_v1 h) {
    if (!call.state||call.state!=call.object) return INFRA_PLUGIN_INVALID_ARGUMENT;
    WorkerClientState *s=(void *)(uintptr_t)call.state;pthread_mutex_lock(&s->lock);WorkerClientOperation *op=find(s,h);
    if (op) {atomic_store(&op->cancelled,1);worker_client_go_cancel(s->service,op->id);}
    pthread_mutex_unlock(&s->lock);return op?INFRA_PLUGIN_OK:INFRA_PLUGIN_STALE_HANDLE;
}
static uint32_t provider_release(InfraObjectCall call,infra_plugin_handle_v1 h) {
    if (!call.state||call.state!=call.object) return INFRA_PLUGIN_INVALID_ARGUMENT;
    WorkerClientState *s=(void *)(uintptr_t)call.state;pthread_mutex_lock(&s->lock);WorkerClientOperation *op=find(s,h);
    if (!op||!op->taken) {pthread_mutex_unlock(&s->lock);return INFRA_PLUGIN_INVALID_STATE;}
    WorkerClientOperation **slot=&s->operations;while (*slot!=op) slot=&(*slot)->next;*slot=op->next;s->count--;pthread_mutex_unlock(&s->lock);
    close(op->fd);free((void *)op->method.ptr);free(op);return INFRA_PLUGIN_OK;
}
static uint32_t provider_close(uint64_t state,uint64_t handle) {
    if (!state||state!=handle) return INFRA_FAILED;
    WorkerClientState *s=(void *)(uintptr_t)state;pthread_mutex_lock(&s->lock);s->closed=1;int pending=s->operations!=NULL;pthread_mutex_unlock(&s->lock);
    if (pending) return INFRA_FAILED;
    return worker_client_go_close(s->service);
}
static uint32_t provider_destroy(uint64_t state) {
    if (provider_close(state,state)!=INFRA_OK) return INFRA_FAILED;
    WorkerClientState *s=(void *)(uintptr_t)state;worker_client_go_delete(s->service);pthread_mutex_destroy(&s->lock);free(s);return INFRA_OK;
}
static void provider_free(uint64_t state,InfraOwnedBytes reply) {(void)state;free(reply.ptr);}
static InfraPluginV1 provider={
 {sizeof(InfraPluginV1),INFRA_PLUGIN_ABI_VERSION,INFRA_PLUGIN_TOOL,INFRA_LANGUAGE_GO,INFRA_UNLOAD_RETAIN,{NULL,0},{(const uint8_t *)"client-v1",sizeof("client-v1")-1}},
 provider_init,provider_open,provider_call,provider_next,provider_close,provider_destroy,provider_free
};
static pthread_once_t identity_once=PTHREAD_ONCE_INIT;
static void initialize_identity(void) { provider.header.package=worker_client_go_package(); }
const InfraPluginV1 *infra_plugin_v1_entry(void){pthread_once(&identity_once,initialize_identity);return &provider;}
uint32_t infra_plugin_v1_query_interface(uint64_t state,uint64_t object,InfraInterfaceHeader requested,uint32_t capacity,void *out){
 if (!state||state!=object||!out||memcmp(requested.id,"infra.async.v001",16)||requested.major!=1||requested.struct_size!=sizeof(InfraAsyncCallV1)||capacity<sizeof(InfraAsyncCallV1)) return INFRA_FAILED;
 InfraAsyncCallV1 table={0};memcpy(table.header.id,"infra.async.v001",16);table.header.major=1;table.header.struct_size=sizeof(table);table.start=provider_start;table.take=provider_take;table.cancel=provider_cancel;table.release=provider_release;memcpy(out,&table,sizeof(table));return INFRA_OK;
}
