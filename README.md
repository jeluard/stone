Does not compete with CEP (like esper). Can be used in // or as a source of info.

## Limitations

* Millisecond granularity. Several data point with same millisecond will be rejected
* Consolidator cannot be added/removed dynamically.
  Complexify implementation (Dispatcher, Storage).