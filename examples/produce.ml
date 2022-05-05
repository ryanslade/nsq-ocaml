open Base
open Lwt
open Lwt.Syntax
open Nsq

let nsqd_address = "localhost"
let publish_error_backoff = 1.0
let to_publish = 500000
let log_interval = 1.0
let start = ref (Unix.gettimeofday ())
let published = ref 0
let concurrency = 5
let batch_size = 20

let publish p =
  let rec loop () =
    let msg = Int.to_string !published in
    let messages =
      List.init batch_size ~f:(fun i ->
          msg ^ ":" ^ Int.to_string i |> Bytes.of_string)
    in
    let* res = Producer.publish_multi p (Topic "Test") messages in
    match res with
    | Result.Ok _ ->
        published := !published + batch_size;
        if !published >= to_publish then Caml.exit 0 else loop ()
    | Result.Error e ->
        let* () = Logs_lwt.err (fun l -> l "%s" e) in
        let* () = Lwt_unix.sleep publish_error_backoff in
        loop ()
  in
  loop ()

let rate_logger () =
  let rec loop () =
    let* () = Lwt_unix.sleep log_interval in
    let published = !published in
    let elapsed = Unix.gettimeofday () -. !start in
    let per_sec = Float.of_int published /. elapsed in
    let* () =
      Logs_lwt.debug (fun l -> l "Published %d, %f/s" published per_sec)
    in
    loop ()
  in
  loop ()

let setup_logging level =
  Logs.set_level level;
  Fmt_tty.setup_std_outputs ();
  Logs.set_reporter (Logs_fmt.reporter ())

let () =
  setup_logging (Some Logs.Debug);
  let p =
    Result.ok_or_failwith
    @@ Producer.create ~pool_size:concurrency (Host nsqd_address)
  in
  let publishers = List.init ~f:(fun _ -> publish p) concurrency in
  start := Unix.gettimeofday ();
  Lwt_main.run @@ join (rate_logger () :: publishers)
