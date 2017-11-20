open Containers
open Lwt
open Nsq

let nsqd_address = "172.17.0.1"
let lookupd_address = "172.17.0.1"

let handle msg =
  Lwt_log.debug_f "Handled Body: %s" (Bytes.to_string msg) >>= fun () ->
  return HandlerOK

let publish_error_backoff = 1.0
let publish_interval_seconds = 1.0

let rec test_publish () =
  let p = match Publisher.create (Host nsqd_address) 
    with 
    | Result.Ok p -> p 
    | Result.Error s -> failwith s in
  let rec loop () =
    let msg = Unix.gettimeofday () |> string_of_float |> Bytes.of_string in
    Lwt_log.debug_f "Publishing: %s" (Bytes.to_string msg) >>= fun () ->
    Publisher.publish p (Topic "Test") msg >>= function
    | Result.Ok _ -> 
      Lwt_unix.sleep publish_interval_seconds >>= loop
    | Result.Error e -> 
      Lwt_log.error e >>= fun () ->
      Lwt_unix.sleep publish_error_backoff >>= test_publish
  in
  loop ()

let create_consumer chan_name handler =
  let dc = Consumer.default_config () in
  let config = Consumer.{dc with max_in_flight = 100;} in
  match Consumer.create 
          ~config
          [(Host nsqd_address)]
          (Topic "Test") 
          (Channel chan_name)
          handler
  with
  | Result.Ok c -> c
  | Result.Error s -> failwith s

let create_lookupd_consumer chan_name handler =
  let dc = Consumer.default_config () in
  let config = Consumer.{dc with max_in_flight = 100;} in
  match Consumer.create_using_nsqlookup
          ~config
          [Host lookupd_address]
          (Topic "Test") 
          (Channel chan_name)
          handler
  with
  | Result.Ok c -> c
  | Result.Error s -> failwith s

let setup_logging level =
  Lwt_log_core.default :=
    Lwt_log.channel
      ~template:"$(date).$(milliseconds) [$(level)] $(message)"
      ~close_mode:`Keep
      ~channel:Lwt_io.stdout
      ();
  Lwt_log_core.add_rule "*" level

let () = 
  setup_logging Lwt_log.Debug;
  let consumer = create_consumer "nsq_consumer" handle in
  let l_consumer = create_lookupd_consumer "lookupd_consumer" handle in
  let running_consumers = List.map Consumer.run [l_consumer;consumer] in
  let p1 = test_publish () in
  Lwt_main.run @@ join (p1 :: running_consumers)

