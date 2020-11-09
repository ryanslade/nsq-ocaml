open Base
open Lwt
open Lwt.Syntax

let check_result prefix r =
  match r with Ok x -> x | Error s -> Alcotest.failf "%s: %s" prefix s

let test_publish_and_consume _ () =
  let open Nsq in
  let address = Address.Host "localhost" in
  let topic = Topic.Topic "Integration" in
  let channel = Channel.Channel "integration" in
  let payload = Bytes.of_string "message" in
  let mvar = Lwt_mvar.create_empty () in
  (* Handler returns payload to mvar *)
  let handler data =
    async (fun () -> Lwt_mvar.put mvar data);
    return `Ok
  in
  let c = Consumer.create [ address ] topic channel handler in
  (* Start consumer in background *)
  async (fun () -> Consumer.run c);
  let p = Producer.create address |> check_result "Creating producer" in
  (* Send single payload *)
  let* res = Producer.publish p topic payload in
  let () = check_result "Single publish" res in
  let* res = Producer.publish_multi p topic [ payload; payload ] in
  let () = check_result "Multi publish" res in
  (* At this point we should have payload queus up three times *)
  let* pub1 = Lwt_mvar.take mvar in
  let* multi1 = Lwt_mvar.take mvar in
  let* multi2 = Lwt_mvar.take mvar in
  Alcotest.(check bytes) "same payload" payload pub1;
  Alcotest.(check bytes) "same payload" payload multi1;
  Alcotest.(check bytes) "same payload" payload multi2;
  return_unit

let () =
  Lwt_main.run
  @@ Alcotest_lwt.run "integration"
       [
         ( "all",
           [
             Alcotest_lwt.test_case "publish and consume" `Quick
               test_publish_and_consume;
           ] );
       ]
