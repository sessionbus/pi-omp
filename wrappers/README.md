# Pi and OMP wrappers

The Pi and OMP Go wrappers use the public Sessionbus Go SDK and share the local `pifamily` bridge. Common host and version helpers come from the pinned `github.com/sessionbus/peer-common` module. There are no imports from daemon internals or from the former combined peers module.
