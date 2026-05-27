open Eio.Std

let check_result prefix r =
  match r with Ok x -> x | Error s -> Alcotest.failf "%s: %s" prefix s

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
        ] );
    ]
