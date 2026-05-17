(lang dune 3.0)

(name nsq)

(version 0.6.0)

(generate_opam_files true)

(source
 (github ryanslade/nsq-ocaml))

(license MIT)

(authors "Ryan Slade <ryanslade@gmail.com>")

(maintainers "Ryan Slade <ryanslade@gmail.com>")

(package
 (name nsq)
 (synopsis "A client library for the NSQ messaging platform")
 (depends
  (ocaml (>= 5.4.1))
  (dune (>= 3.0))
  core
  (eio (>= 1.0))
  (eio_main (>= 1.0))
  cohttp
  cohttp-eio
  yojson
  ppx_deriving_yojson
  ppx_jane
  logs
  fmt
  hex
  (alcotest :with-test)))
