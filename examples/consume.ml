open Eio.Std
open Nsq

let nsqd_address = "localhost"
let log_interval = 1.0
let start = ref 0.0
let expected = 500000
let consumed = ref 0
let in_flight = 100

let rate_logger ~clock () =
  let rec loop () =
    Eio.Time.sleep clock log_interval;
    let consumed = !consumed in
    let elapsed = Eio.Time.now clock -. !start in
    let per_sec = Float.of_int consumed /. elapsed in
    Logs.debug (fun l -> l "Consumed %d, %f/s" consumed per_sec);
    if consumed >= expected then exit 0 else loop ()
  in
  loop ()

let setup_logging level =
  Logs.set_level level;
  Fmt_tty.setup_std_outputs ();
  Logs.set_reporter (Logs_fmt.reporter ())

let handler _ =
  incr consumed;
  `Ok

let () =
  setup_logging (Some Logs.Debug);
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    Consumer.Config.create ~max_in_flight:in_flight () |> Result.get_ok
  in
  let consumer =
    Consumer.create ~net ~clock ~mode:`Nsqd ~config [ Host nsqd_address ]
      (Topic "Test") (Channel "benchmark") handler
  in
  start := Eio.Time.now clock;
  Fiber.all [ rate_logger ~clock; (fun () -> Consumer.run consumer) ]
