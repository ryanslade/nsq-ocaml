open Base
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
  let stream = Eio.Stream.create Int.max_value in
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

let () =
  Eio_main.run @@ fun env ->
  Alcotest.run "integration"
    [
      ( "all",
        [
          Alcotest.test_case "publish and consume" `Quick
            (test_publish_and_consume env);
        ] );
    ]
