open Base
open Eio.Std
open Nsq

let nsqd_address = "localhost"
let publish_error_backoff = 1.0
let to_publish = 500000
let log_interval = 1.0
let start = ref 0.0
let published = ref 0
let concurrency = 5
let batch_size = 20

let publish ~clock p =
  let rec loop () =
    let msg = Int.to_string !published in
    let messages =
      List.init batch_size ~f:(fun i ->
          msg ^ ":" ^ Int.to_string i |> Bytes.of_string)
    in
    match Producer.publish_multi p (Topic "Test") messages with
    | Result.Ok _ ->
        published := !published + batch_size;
        if !published >= to_publish then Stdlib.exit 0 else loop ()
    | Result.Error e ->
        Logs.err (fun l -> l "%s" e);
        Eio.Time.sleep clock publish_error_backoff;
        loop ()
  in
  loop ()

let rate_logger ~clock () =
  let rec loop () =
    Eio.Time.sleep clock log_interval;
    let published = !published in
    let elapsed = Eio.Time.now clock -. !start in
    let per_sec = Float.of_int published /. elapsed in
    Logs.debug (fun l -> l "Published %d, %f/s" published per_sec);
    loop ()
  in
  loop ()

let setup_logging level =
  Logs.set_level level;
  Fmt_tty.setup_std_outputs ();
  Logs.set_reporter (Logs_fmt.reporter ())

let () =
  setup_logging (Some Logs.Debug);
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  let p =
    Result.ok_or_failwith
    @@ Producer.create ~sw ~net ~clock ~pool_size:concurrency
         (Host nsqd_address)
  in
  start := Eio.Time.now clock;
  Fiber.all
    (rate_logger ~clock
    :: List.init concurrency ~f:(fun _ () -> publish ~clock p))
