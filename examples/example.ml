open Base
open Eio.Std
open Nsq

let nsqd_address = "localhost"
let lookupd_address = "localhost"

let make_handler name msg =
  Logs.debug (fun l -> l "(%s) Handled Body: %s" name (Bytes.to_string msg));
  `Ok

let publish_error_backoff = 1.0
let publish_interval_seconds = 1.0

let test_publish ~sw ~net ~clock () =
  let p =
    Result.ok_or_failwith @@ Producer.create ~sw ~net ~clock (Host nsqd_address)
  in
  let rec loop () =
    let msg = Eio.Time.now clock |> Float.to_string |> Bytes.of_string in
    Logs.debug (fun l -> l "Publishing: %s" (Bytes.to_string msg));
    match Producer.publish p (Topic "Test") msg with
    | Result.Ok _ ->
        Eio.Time.sleep clock publish_interval_seconds;
        loop ()
    | Result.Error e ->
        Logs.err (fun l -> l "%s" e);
        Eio.Time.sleep clock publish_error_backoff;
        loop ()
  in
  loop ()

let create_consumer ~net ~clock ~mode chan_name handler =
  let config =
    Consumer.Config.create ~max_in_flight:100
      ~lookupd_poll_interval:(Seconds.of_float 60.0) ()
    |> Result.ok_or_failwith
  in
  let host_port =
    match mode with
    | `Nsqd -> Address.Host nsqd_address
    | `Lookupd -> Host lookupd_address
  in
  Consumer.create ~net ~clock ~mode ~config [ host_port ] (Topic "Test")
    (Channel chan_name) handler

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
  let consumer =
    create_consumer ~net ~clock ~mode:`Nsqd "nsq_consumer" (make_handler "nsq")
  in
  let l_consumer =
    create_consumer ~net ~clock ~mode:`Lookupd "lookupd_consumer"
      (make_handler "lookupd")
  in
  Fiber.all
    [
      test_publish ~sw ~net ~clock;
      (fun () -> Consumer.run l_consumer);
      (fun () -> Consumer.run consumer);
    ]
