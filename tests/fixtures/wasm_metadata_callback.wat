(module
  ;; Guest-owned replies and reusable scratch; no host imports or WASI.
  (memory (export "memory") 2 2)
  (data (i32.const 0) "\7b\22\61\62\69\22\3a\31\2c\22\6b\69\6e\64\22\3a\22\6f\62\6a\65\63\74\22\2c\22\70\61\63\6b\61\67\65\22\3a\22\77\6f\72\6b\65\72\2e\77\61\73\6d\2e\6d\65\74\61\64\61\74\61\2e\74\65\73\74\22\2c\22\76\61\72\69\61\6e\74\22\3a\22\77\61\73\6d\22\7d")
  (data (i32.const 256) "\7b\22\76\65\72\73\69\6f\6e\22\3a\31\2c\22\64\65\63\69\73\69\6f\6e\22\3a\22\63\6f\6e\74\69\6e\75\65\22\2c\22\73\65\74\5f\68\65\61\64\65\72\73\22\3a\5b\5b\22\78\2d\70\6f\6c\69\63\79\22\2c\22\77\61\73\6d\2d\61\70\70\72\6f\76\65\64\22\5d\5d\7d")
  (func (export "infra_descriptor") (result i64)
    i64.const 80)
  (func (export "infra_buffer") (result i64)
    i64.const 17592186109952)
  (func (export "infra_open") (param i32) (result i64)
    i64.const 1)
  (func (export "infra_call") (param $handle i64) (param $method_len i32) (param $input_len i32) (result i64)
    ;; The host must dispatch the registered method and a real metadata frame.
    local.get $handle
    i64.const 1
    i64.ne
    if unreachable end
    local.get $method_len
    i32.const 11
    i32.ne
    if unreachable end
    i32.const 4096
    i32.load8_u
    i32.const 112 ;; 'p' in pre_routing
    i32.ne
    if unreachable end
    local.get $input_len
    i32.eqz
    if unreachable end
    i32.const 4107
    i32.load8_u
    i32.const 123 ;; metadata JSON starts with '{'
    i32.ne
    if unreachable end
    i64.const 1099511627856)
  (func (export "infra_drop") (param i64))
)
