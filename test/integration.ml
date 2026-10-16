open Eio.Std

let check_result prefix r =
  match r with Ok x -> x | Error s -> Alcotest.failf "%s: %s" prefix s

let contains s sub =
  let n = String.length sub in
  let rec go i =
    i + n <= String.length s && (String.sub s i n = sub || go (i + 1))
  in
  go 0

let test_publish_and_consume env () =
  let open Nsq in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  let address = Address.Host "localhost" in
  let topic = Topic.Topic "Integration" in
  let channel = Channel.Channel "integration" in
  let payload = Bytes.of_string "message" in
  let stream = Eio.Stream.create Int.max_int in
  (* Handler returns payload to stream *)
  let handler data =
    Eio.Stream.add stream data;
    `Ok
  in
  let c = Consumer.create ~net ~clock [ address ] topic channel handler in
  let test () =
    let p =
      Producer.create ~sw ~net ~clock address
      |> check_result "Creating producer"
    in
    (* Send single payload *)
    let res = Producer.publish p topic payload in
    let () = check_result "Single publish" res in
    let res = Producer.publish_multi p topic [ payload; payload ] in
    let () = check_result "Multi publish" res in
    (* At this point we should have payload queued up three times *)
    let pub1 = Eio.Stream.take stream in
    let multi1 = Eio.Stream.take stream in
    let multi2 = Eio.Stream.take stream in
    Alcotest.(check bytes) "same payload" payload pub1;
    Alcotest.(check bytes) "same payload" payload multi1;
    Alcotest.(check bytes) "same payload" payload multi2
  in
  Fiber.first (fun () -> Consumer.run c) test

let test_lookupd_mode env () =
  let open Nsq in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  let nsqd_address = Address.Host "localhost" in
  let lookupd_address = Address.HostPort ("localhost", 4161) in
  let topic = Topic.Topic "IntegrationLookupd" in
  let channel = Channel.Channel "lookupd_test" in
  let payload = Bytes.of_string "lookupd message" in
  let stream = Eio.Stream.create Int.max_int in
  let handler data =
    Eio.Stream.add stream data;
    `Ok
  in
  let config =
    Consumer.Config.create ~lookupd_poll_interval:(Seconds.of_float 1.0) ()
    |> check_result "Config"
  in
  let c =
    Consumer.create ~net ~clock ~mode:`Lookupd ~config [ lookupd_address ] topic
      channel handler
  in
  let test () =
    let p =
      Producer.create ~sw ~net ~clock nsqd_address
      |> check_result "Creating producer"
    in
    (* Publish so the topic exists and nsqd registers it with lookupd *)
    let () = check_result "Publish" (Producer.publish p topic payload) in
    (* Republish periodically so we don't race lookupd's discovery interval. *)
    let publisher () =
      while true do
        Eio.Time.sleep clock 1.0;
        let _ = Producer.publish p topic payload in
        ()
      done
    in
    let consumer () =
      let received = Eio.Stream.take stream in
      Alcotest.(check bytes) "received via lookupd" payload received
    in
    Fiber.first publisher consumer
  in
  Fiber.first (fun () -> Consumer.run c) test

let test_requeue env () =
  let open Nsq in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  let address = Address.Host "localhost" in
  let topic = Topic.Topic "IntegrationRequeue" in
  let channel = Channel.Channel "requeue" in
  let payload = Bytes.of_string "requeue me" in
  let call_count = Atomic.make 0 in
  let done_stream = Eio.Stream.create 1 in
  let handler data =
    let n = Atomic.fetch_and_add call_count 1 + 1 in
    if n = 1 then `Requeue
    else begin
      Eio.Stream.add done_stream data;
      `Ok
    end
  in
  (* Bump error_threshold so a single requeue doesn't trip the breaker. *)
  let config =
    Consumer.Config.create ~error_threshold:100 () |> check_result "Config"
  in
  let c =
    Consumer.create ~net ~clock ~config [ address ] topic channel handler
  in
  let test () =
    let p =
      Producer.create ~sw ~net ~clock address
      |> check_result "Creating producer"
    in
    let () = check_result "Publish" (Producer.publish p topic payload) in
    (* Requeue delay is default_requeue_delay (5s) * attempts; first
       requeue waits ~5 seconds before redelivery. *)
    let received = Eio.Stream.take done_stream in
    Alcotest.(check bytes) "redelivered payload" payload received;
    Alcotest.(check int) "handler called twice" 2 (Atomic.get call_count)
  in
  Fiber.first (fun () -> Consumer.run c) test

(* Runs a publish that nsqd rejects and checks the error, then checks the next
   publish succeeds. The pool has one connection, so the second publish would
   reuse the connection nsqd closed if it were kept. *)
let check_publish_recovers env ~bad_publish ~check_error () =
  let open Nsq in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  let p =
    Producer.create ~sw ~net ~clock ~pool_size:1 (Address.Host "localhost")
    |> check_result "Creating producer"
  in
  (match bad_publish p with
  | Ok () -> Alcotest.fail "Expected publish to fail"
  | Error e -> check_error e);
  let res =
    Producer.publish p (Topic.Topic "IntegrationAfterError")
      (Bytes.of_string "ok")
  in
  check_result "Publish after error" res

let check_error_mentions sub e =
  Alcotest.(check bool)
    (Printf.sprintf "%S mentions %s" e sub)
    true (contains e sub)

let test_publish_after_bad_topic env =
  check_publish_recovers env
    ~bad_publish:(fun p ->
      Nsq.Producer.publish p (Nsq.Topic.Topic "bad!topic")
        (Bytes.of_string "nope"))
    ~check_error:(check_error_mentions "ErrorBadTopic")

let test_publish_after_empty_mpub env =
  check_publish_recovers env
    ~bad_publish:(fun p ->
      Nsq.Producer.publish_multi p (Nsq.Topic.Topic "IntegrationEmptyMpub") [])
    ~check_error:(check_error_mentions "E_BAD_BODY")

let test_publish_after_oversized_message env =
  (* One byte over nsqd's default --max-msg-size *)
  let body = Bytes.make ((1024 * 1024) + 1) 'x' in
  check_publish_recovers env
    ~bad_publish:(fun p ->
      Nsq.Producer.publish p (Nsq.Topic.Topic "IntegrationOversized") body)
      (* nsqd replies E_BAD_MESSAGE and closes before reading the body, so
         depending on timing we see that error or a connection reset *)
    ~check_error:ignore

(* A fake nsqd listening on a random loopback port. [handle] is run for each
   accepted connection along with its index, starting at 0, and the connection
   is closed when it returns. *)
let fake_nsqd ~sw ~net handle =
  let socket =
    Eio.Net.listen ~sw ~backlog:5 ~reuse_addr:true net
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, 0))
  in
  let port =
    match Eio.Net.listening_addr socket with
    | `Tcp (_, port) -> port
    | _ -> Alcotest.fail "Expected a TCP listening address"
  in
  Fiber.fork_daemon ~sw (fun () ->
      let rec loop i =
        Eio.Net.accept_fork ~sw socket
          ~on_error:(fun _ -> ())
          (fun flow _ ->
            handle i flow (Eio.Buf_read.of_flow ~max_size:(1024 * 1024) flow));
        loop (i + 1)
      in
      loop 0);
  Nsq.Address.HostPort ("127.0.0.1", port)

let read_sized_body r =
  let size = Eio.Buf_read.BE.uint32 r |> Int32.to_int in
  ignore (Eio.Buf_read.take size r)

let read_magic r = ignore (Eio.Buf_read.take 4 r)

(* IDENTIFY\n, PUB <topic>\n and MPUB <topic>\n are all followed by a sized body *)
let read_command_with_body r =
  ignore (Eio.Buf_read.line r);
  read_sized_body r

let write_ok flow =
  let buf = Buffer.create 10 in
  Buffer.add_int32_be buf 6l;
  Buffer.add_int32_be buf 0l;
  Buffer.add_string buf "OK";
  Eio.Flow.copy_string (Buffer.contents buf) flow

(* Read and ignore everything until the client closes the connection *)
let drain r = try Eio.Buf_read.skip max_int r with End_of_file -> ()

let test_consumer_reconnects_after_silence env () =
  let open Nsq in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  let connections = Eio.Stream.create 10 in
  (* Complete the handshake, then never send anything again. Without a read
     timeout the consumer would wait on this connection forever. *)
  let address =
    fake_nsqd ~sw ~net (fun i flow r ->
        Eio.Stream.add connections i;
        read_magic r;
        read_command_with_body r;
        write_ok flow;
        ignore (Eio.Buf_read.line r);
        (* SUB *)
        write_ok flow;
        drain r)
  in
  let config =
    Consumer.Config.create ~heartbeat_interval:(Seconds.of_float 1.0)
      ~read_timeout:(Seconds.of_float 1.5) ()
    |> check_result "Config"
  in
  let c =
    Consumer.create ~net ~clock ~config [ address ] (Topic.Topic "Silent")
      (Channel.Channel "silent") (fun _ -> `Ok)
  in
  let test () =
    Eio.Time.with_timeout_exn clock 30.0 (fun () ->
        Alcotest.(check int) "first connection" 0 (Eio.Stream.take connections);
        Alcotest.(check int) "reconnected" 1 (Eio.Stream.take connections))
  in
  Fiber.first (fun () -> Consumer.run c) test

let test_producer_reconnects_after_drop env () =
  let open Nsq in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  (* The first connection is closed after reading a publish, without replying.
     Later connections reply OK. *)
  let address =
    fake_nsqd ~sw ~net (fun i flow r ->
        read_magic r;
        read_command_with_body r;
        if i > 0 then write_ok flow)
  in
  Switch.run @@ fun sw ->
  let p =
    Producer.create ~sw ~net ~clock ~pool_size:1 address
    |> check_result "Creating producer"
  in
  let topic = Topic.Topic "Dropped" in
  let payload = Bytes.of_string "message" in
  (match Producer.publish p topic payload with
  | Ok () -> Alcotest.fail "Expected publish on dropped connection to fail"
  | Error _ -> ());
  check_result "Publish after drop" (Producer.publish p topic payload)

let () =
  Eio_main.run @@ fun env ->
  Alcotest.run "integration"
    [
      ( "all",
        [
          Alcotest.test_case "publish and consume" `Quick
            (test_publish_and_consume env);
          Alcotest.test_case "lookupd discovery" `Quick (test_lookupd_mode env);
          Alcotest.test_case "requeue redelivery" `Slow (test_requeue env);
          Alcotest.test_case "publish after bad topic" `Quick
            (test_publish_after_bad_topic env);
          Alcotest.test_case "publish after empty multi publish" `Quick
            (test_publish_after_empty_mpub env);
          Alcotest.test_case "publish after oversized message" `Quick
            (test_publish_after_oversized_message env);
          Alcotest.test_case "producer reconnects after dropped connection"
            `Quick
            (test_producer_reconnects_after_drop env);
          Alcotest.test_case "consumer reconnects after silent connection" `Slow
            (test_consumer_reconnects_after_silence env);
        ] );
    ]
